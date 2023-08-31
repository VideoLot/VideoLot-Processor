import express from 'express';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import cors from 'cors';
import { spawn, spawnSync } from 'child_process';
import pkg from '@prisma/client';
import os from 'os'
import { getMappingsForInCodec } from './mime-map.js';
import { createStorageApi } from 'videolee-cloud-wrapper';
import fastFolderSize from 'fast-folder-size';
import { promisify } from 'util';
import bodyParser from 'body-parser';

const { PrismaClient, StageStatus, UploadStage } = pkg;

dotenv.config();
const mediaStorage = createStorageApi();

const port = 3030;
const app = express();
const rwAccess = fs.constants.R_OK | fs.constants.W_OK;
const TEMP_FOLDER = process.env.TEMP_FILES_FOLDER;
const PROBE_OPTIONS = ['-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', '-i'];
const prisma = new PrismaClient();

fs.access(TEMP_FOLDER, rwAccess, (error) => {
    if (fs.existsSync(TEMP_FOLDER)) {
        if (error) {
            console.error(`Can't access location ${TEMP_FOLDER}`);
            process.exit(-1);
        }
    }

    try {
        fs.mkdirSync(TEMP_FOLDER, { recursive: true});
    } catch (createError) {
        console.error(`Can't create temp files folder by path ${TEMP_FOLDER}: `, createError);
    }
});

app.use(cors());
app.use(bodyParser.raw({type: 'application/octet-stream', limit: '5mb'}));

app.get('/', (req, res) => {
    res.send('server is running');
});

app.post('/', async (req, res) => {
    const {id, filename, currentChunkIndex, totalChunks} = req.query;
    const chunkIndex = parseInt(currentChunkIndex);
    const totalChunk = parseInt(totalChunks);
    const ext = filename.split('.').slice(-1);
    const isFirstChunk = chunkIndex === 0;
    const isLastChunk = chunkIndex === totalChunk - 1;

    const basePath = path.join(TEMP_FOLDER, id);
    const fullPath = path.join(basePath, `source.${ext}`);
    if (isFirstChunk) {
        fs.access(basePath, rwAccess, (error) => {
            if (error) {
                console.warn(`Can't access location ${basePath}`);
                res.sendStatus(500);
                return;
            }
        });

        if (!fs.existsSync(basePath)) {
            fs.mkdirSync(basePath, { recursive: true });
        }

        if (fs.existsSync(fullPath)) {
            fs.unlinkSync(fullPath);
        }
    }

    let progress = 0;
    let stageStatus = StageStatus.InProgress;
    let httpStatus = 200;
    try {
        progress = currentChunkIndex / totalChunk;
        const data = req.body.toString().split(',')[1];
        const buffer = Buffer.from(data, 'base64');
        fs.appendFileSync(fullPath, buffer);// new Uint8Array(buffer));
        if (isLastChunk) {
            stageStatus = StageStatus.Complete;
            progress = 1;
        }
    } catch(e) {
        console.warn(e);
        stageStatus = StageStatus.Failed;
        httpStatus = 500;
    } finally {
        console.log('PROGRESS: ', chunkIndex, 'of', totalChunks);
        await updateUploadState(id, UploadStage.Uploading, stageStatus, fullPath, progress);
        res.sendStatus(httpStatus);

        if (isLastChunk) {
            startProcessing(id);
        }
    }
});

app.post('/start/processing/:id', async (req, res) => {
    const { id } = req.params;

    startProcessing(id);
    return res.status(200);
});

app.post('/start/deploy/:id', async (req, res) => {
    const { id } = req.params;
    const uploadState = await prisma.uploadState.findFirst(
        {where: { videoDataId: id }}
    );

    const { tracks } = await constructMediaData(id, uploadState.filePath, UploadStage.Deploying);
    uploadToStorage(id, tracks);
    res.status(200);
});

app.listen(port, () => {
    console.log(`App started on port ${port}`);
});

async function startProcessing(videoId) {
    const uploadState = await prisma.uploadState.findFirst(
        {where: { videoDataId: videoId }}
    );

    if (!fs.existsSync(uploadState.filePath)) {
        console.warn('File not found', uploadState.filePath);
        await updateUploadState(videoId, UploadStage.Processing, StageStatus.Failed, null, 0);
        return;
    }

    const {tracks, duration} = await constructMediaData(videoId, uploadState.filePath, UploadStage.Processing);
    if (!tracks) {
        await updateUploadState(videoId, UploadStage.Processing, StageStatus.Failed, null, 0);
        return;
    }

    const args = buildFFmpegArguments(tracks, uploadState.filePath, path.join(TEMP_FOLDER, videoId));
    console.log(videoId, 'ffmpeg starting with arguments: ', args.join(' '));
    const ffmpeg = spawn('ffmpeg', args);
    ffmpeg.stdout.on('data', handleFfmpegProgress(videoId, duration, uploadState.filePath));

    let errors = [];
    ffmpeg.stderr.on('data', (data) => {
        errors.push(data.toString('utf-8'));
    });
    ffmpeg.on('close', handleFfmpegFinish(errors, videoId, tracks));
}

function handleFfmpegProgress(videoId, fullLengthMs, filePath) {
    const OUT_TIME = 'out_time_us';
    let previousProgress = 0;

    return async (data) => {
        const stringData = data.toString('utf8');
        const lines = stringData.split(os.EOL);
        for(const line of lines) {
            const decl = line.split('=');
            if (decl.length !== 2) {
                return;
            }

            const name = decl[0].trim();
            const value = decl[1].trim();
            if(name === OUT_TIME) {
                const time = parseInt(value) / 1000;
                const progressNow = time / fullLengthMs;
                if (previousProgress === 0 || progressNow - previousProgress > 0.01) {
                    previousProgress = progressNow;
                    await updateUploadState(videoId, UploadStage.Processing, StageStatus.InProgress, filePath, progressNow);
                }
            }
        }
    };
}

function handleFfmpegFinish(errors, videoId, tracks) {
    return async (code) => {
        console.log('FINISHED: ', code);
        if (code !== 0) {
            console.error(errors.join(os.EOL), os.EOL, videoId, UploadStage.Processing, 'Error while invoking ffmpeg: ', code);
            await updateUploadState(videoId, UploadStage.Processing, StageStatus.Failed, null, null);
            return;
        }

        console.log(videoId, UploadStage.Processing, 'Processing of video successfully finished');
        await updateUploadState(videoId, UploadStage.Processing, StageStatus.Complite, null, 1);
        uploadToStorage(videoId, tracks)
    }
}

async function uploadToStorage(videoId, tracks) {
    const folderSize = promisify(fastFolderSize);
    let totalSize = 0;
    for (const track of tracks) {
        const size = await folderSize(track.localPath);
        totalSize += size | 0;
    }
    
    console.log(videoId, 'Staring deploying of video, total size: ', totalSize) ;
    await updateUploadState(videoId, UploadStage.Deploying, StageStatus.InProgress, null, 0);

    let uploadedSize = 0;
    let lastReportedProgress = 0;
    for(const track of tracks) {
        const files = fs.readdirSync(track.localPath);
        await prisma.trackInfo.update({
            where: {id: track.track.trackInfo.id},
            data: {
                segmentsCount: files.length
            }
        });
        for (const file of files) {
            const fullFilePath = path.join(track.localPath, file);
            const fileURI = mediaStorage.createPath(track.track.trackInfo.trackPath, file); // The longest track chain ever

            const stream = fs.createReadStream(fullFilePath);
            const putResult = await mediaStorage.putObject(stream, fileURI);

            if (putResult.status === 'FAILED') {
                console.warn(videoId, `Failed while deploying file ${fullFilePath}: `, putResult.error);
                await updateUploadState(videoId, UploadStage.Deploying, StageStatus.Failed, null, 0);
                return;
            }

            const fileStat = fs.statSync(fullFilePath);
            uploadedSize += fileStat.size; 
            let progress =  uploadedSize / totalSize;
            if (progress - lastReportedProgress >= 0.01) {
                lastReportedProgress = progress;
                await updateUploadState(videoId, UploadStage.Deploying, StageStatus.InProgress, null, progress);
            }
        }
    }
    await updateUploadState(videoId, UploadStage.Deploying, StageStatus.Complite, null, 1);
}

async function updateUploadState(videoId, stage, status, path, progress) {
    console.log(videoId, `${stage} PROGRESS: `, progress);
    const updateData = {};
    if (stage) {
        updateData['stage'] = stage;
    }
    if (status) {
        updateData['stageStage'] = status;
    }
    if (path) {
        updateData['filePath'] = path;
    }
    if (progress) {
        updateData['progress'] = progress;
    }

    await prisma.uploadState.upsert({
        where:{videoDataId: videoId},
        update: updateData,
        create: {
            videoDataId: videoId,
            stage: stage || UploadStage.Uploading,
            stageStage: status || StageStatus.InProgress,
            filePath: path || '',
            progress: progress || 0
        }
    })
}

function buildFFmpegArguments(tracks, inputFile, outPath) {
    const segmentsArgs = ['segment', '-segment_time', '2'];
    const videoKF = ['-force_key_frames', 'expr:gte(t,n_forced*2)'];

    const result = ['-progress', '-', '-nostats', '-i', inputFile];
    for (const track of tracks) {
        const streamMaps = ['-map', `0:${track.stream.index}`, '-f'];
        let outFile = '';
        if (track.stream.codec_type === 'video') {
            result.push(...streamMaps, ...segmentsArgs, ...videoKF, '-c:v', 'vp9');
            outFile = `part%d.webm`;
        }
        if (track.stream.codec_type === 'audio') {
            result.push(...streamMaps, ...segmentsArgs);
            outFile = `part%d.${track.stream.codec_name}`
        }
        const fullPath = path.join(track.localPath, outFile);
        result.push(fullPath);
    }
    return result;
}

async function constructMediaData(videoId, sourcePath, stage) {
    const probeArgs = [...PROBE_OPTIONS, sourcePath];
    console.log('Invoking ffprobe with args: ', probeArgs.join(' '));
    const probeResult = spawnSync(`ffprobe`, probeArgs);

    if(probeResult.error) {
        console.warn('Error while invoking ffprobe: ', probeResult.error);
        updateUploadState(videoId, stage, StageStatus.Failed, null, 0);
        return;
    }

    const probeData = JSON.parse(probeResult.stdout.toString('utf-8'));
    if (!probeData || !probeData.streams || !probeData.format) {
        console.warn('Error while parsing ffprobe output: ', probeData);
        updateUploadState(videoId, stage, StageStatus.Failed, null, 0);
        return;
    }
    const duration = parseFloat(probeData.format.duration) * 1000;

    const tracks = await createTracks(videoId, probeData.streams);
    createTracksPaths(videoId, tracks);
    return {tracks, duration};
}

function createTracksPaths (videoId, tracks) {
    let audioTrackNumber = 1;
    for (const track of tracks) {
        let outDir;
        if (track.stream.codec_type === 'video') {
            outDir = ['video', '1080'];
        }
        if (track.stream.codec_type === 'audio') {
            outDir = ['audio', `track${audioTrackNumber}`]
            audioTrackNumber++;
        }

        if (outDir) {
            const localPath = path.join(TEMP_FOLDER, videoId, ...outDir);
            if (!fs.existsSync(localPath)) {
                fs.mkdirSync(localPath, {recursive: true});
            }
            track.localPath = localPath;
        }
    }
}

async function createTracks(videoId, streams) {
    let audioTrackNumber = 1;
    const createVideoTrackInfo = (stream) => {
        return {
            create: {
                segmentsCount: 0,
                duration: Math.floor(parseFloat(stream.duration) * 1000),
                codec: 'video/webm;codecs=vp9',
                trackPath: mediaStorage.createPath(videoId, 'video', '1080'),
                quality: stream.height.toString()
        }};
    };
    const createAudioTrackInfo = (stream) => {
        const mapping = getMappingsForInCodec(stream.codec_name);
        const data = { 
            create: {
                segmentsCount: 0,
                duration: Math.floor(parseFloat(stream.duration) * 1000),
                codec: mapping.mime,
                trackPath: mediaStorage.createPath(videoId, 'audio', `track${audioTrackNumber}`),
                quality: stream.bit_rate
        }};
        audioTrackNumber++;
        return data;
    }

    const videoTracks = await prisma.videoTrack.findMany({
        where: { videoDataId: videoId },
        include: {
            trackInfo: true
        }
    });

    const audioTracks = await prisma.audioTrack.findMany({
        where: { videoDataId: videoId },
        include: {
            trackInfo: true
        }
    });

    const tracks = [];
    for (const stream of streams) {
        let track;
        if (stream.codec_type === 'video') {
            const creationRequest = createVideoTrackInfo(stream);
            const exists = videoTracks.find( x=> x.trackInfo.trackPath === creationRequest.create.trackPath);

            if (exists) {
                track = exists;
            } else {
                track = await prisma.videoTrack.create({ 
                    data: {
                        videoData: {connect: {id: videoId}},
                        trackInfo: creationRequest
                    },
                    include: {
                        trackInfo: true
                    }
                });
            }
        }
        if (stream.codec_type === 'audio') {
            const creationRequest = createAudioTrackInfo(stream);
            const exists = audioTracks.find( x=> x.trackInfo.trackPath === creationRequest.create.trackPath);
            if (exists) {
                track = exists;
            } else {
                track = await prisma.audioTrack.create({
                    data: {
                        videoData: {connect: {id: videoId}},
                        trackInfo: creationRequest
                    },
                    include: {
                        trackInfo: true
                    }
                });
            }
            
        }
        tracks.push({
            stream,
            track
        });
    }
    return tracks;
}