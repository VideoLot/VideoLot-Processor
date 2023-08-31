const CODEC_MIME_EXTENSION = [
    {inCodec: 'mp3',    outCodec: 'mp3', mime: 'audio/mpeg',            extension: '.mp3'},
    {inCodec: 'ac3',    outCodec: 'ac3', mime: 'audio/ac3',             extension: '.ac3'},
    {inCodec: 'h264',   outCodec: 'vp9', mime: 'video/webm;codecs=vp8', extension: '.webm'}
];

export function getMappingsForInCodec(codec) {
    const mapping = CODEC_MIME_EXTENSION.find(x=>x.inCodec === codec);
    return mapping ? mapping : null;
}