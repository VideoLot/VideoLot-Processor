import express from 'express';

const port = 3030;
const app = express();

app.get('/', (req, res) => {
    res.send('server is running');
});

app.listen(port, () => {
    console.log(`App started on port ${port}`);
});