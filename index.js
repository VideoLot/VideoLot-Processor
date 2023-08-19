import express from 'express';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import cors from 'cors';
import { spawn, spawnSync } from 'child_process';
import pkg from '@prisma/client';
import os from 'os'
import { EOF } from 'dns';
const { PrismaClient, StageStatus, UploadStage } = pkg;

dotenv.config();

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
app.use(express.raw({type: 'application/octet-stream'}));

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
        const data = req.body.split(',')[1];
        const buffer = Buffer.from(data, 'base64');
        fs.appendFileSync(fullPath, buffer);
        if (isLastChunk) {
            stageStatus = StageStatus.Complete;
            progress = 1;
        }
    } catch(e) {
        console.warn(e);
        stageStatus = StageStatus.Failed;
        httpStatus = 500;
    } finally {
        const data = {
            videoDataId: id,
            stage: UploadStage.Uploading,
            stageStage: stageStatus,
            filePath: fullPath,
            progress
        };
        await prisma.uploadState.upsert({
            select: {where: {videoDataId: id}},
            data: data
        });
        res.body = data;
        res.sendStatus(httpStatus);
    }
});

app.post('/start/:id', async (req, res) => {
    const { id } = req.params;
    const uploadState = await prisma.uploadState.findFirst(
        {where: { videoDataId: id }}
    );

    if (!fs.existsSync(uploadState.filePath)) {
        res.sendStatus(404);
        return;
    }

    const probeResult = spawnSync(`ffprobe`, [...PROBE_OPTIONS, uploadState.filePath]);

    if(probeResult.error) {
        console.warn('Error while invoking ffprobe: ', probeResult.error);
        res.sendStatus(500);
        return;
    }

    const probeData = JSON.parse(probeResult.stdout.toString('utf-8'));
    if (!probeData) {
        console.warn('Error while parsing probe result');
        res.sendStatus(500);
        return;
    }
    const duration = parseFloat(probeData.format.duration) * 1000;

    let errors = [];
    const args = buildCLIarguments(probeData, uploadState.filePath, path.join(TEMP_FOLDER, id));
    console.log(id, 'ffmpeg starting with arguments: ', args.join(' '));
    const ffmpeg = spawn('ffmpeg', args);
    ffmpeg.stdout.on('data', handleFfmpegProgress(id, duration, uploadState.filePath));
    ffmpeg.stderr.on('data', (data) => {
        errors.push(data.toString('utf-8'));
    });
    ffmpeg.on('close', (data) => handleFfmpegFinish(errors, id));

    res.sendStatus(200);
});

app.listen(port, () => {
    console.log(`App started on port ${port}`);
});

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

function handleFfmpegFinish(errors, videoId) {
    return async (code) => {
        if (code !== 0) {
            console.error(errors.join(os.EOL), os.EOL, videoId, UploadStage.Processing, 'Error while invoking ffmpeg: ', code);
            await updateUploadState(videoId, UploadStage.Processing, StageStatus.Failed, null, null);
            return;
        }

        console.log(videoId, UploadStage.Processing, 'Processing of video successfully finished');
        await updateUploadState(videoId, UploadStage.Processing, StageStatus.Complite, null, 1);
    }
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

function buildCLIarguments(data, inputFile, outPath) {
    const segmentsArgs = ['segment', '-segment_time', '2'];
    const videoKF = ['-force_key_frames', 'expr:gte(t,n_forced*2)'];

    const result = ['-progress', '-', '-nostats', '-i', inputFile];
    let audioTrackNumber = 1;
    for (const stream of data.streams) {
        const streamMaps = ['-map', `0:${stream.index}`, '-f'];
        let outFile = '';
        if (stream.codec_type === 'video') {
            result.push(...streamMaps, ...segmentsArgs, ...videoKF, '-c:v', 'vp9');
            outFile = `video/1080/part%d.webm`;
        }
        if (stream.codec_type === 'audio') {
            result.push(...streamMaps, ...segmentsArgs);
            outFile = `audio/track${audioTrackNumber}/part%d.${stream.codec_name}`
            audioTrackNumber++;
        }
        const fullPath = path.join(outPath, outFile);
        result.push(fullPath);

        const basePath = path.dirname(fullPath);
        if (!fs.existsSync(basePath)) {
            console.log('BASE PATH: ', basePath);
            fs.mkdirSync(basePath, {recursive: true});
        }
    }
    return result;
}