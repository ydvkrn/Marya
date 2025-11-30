const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const app = express();

// CORS enable karo
app.use(cors());

// Body size limits increase karo
app.use(express.json({ limit: '500mb' }));
app.use(express.urlencoded({ limit: '500mb', extended: true }));

// Uploads folder create karo agar nahi hai toh
const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)) {
    fs.mkdirSync(uploadsDir, { recursive: true });
}

// Multer configuration for chunked uploads
const storage = multer.diskStorage({
    destination: (req, res, cb) => {
        cb(null, uploadsDir);
    },
    filename: (req, file, cb) => {
        // Original filename with timestamp
        const timestamp = Date.now();
        const originalName = file.originalname;
        cb(null, `${timestamp}-${originalName}`);
    }
});

// Multer with increased limits
const upload = multer({
    storage: storage,
    limits: {
        fileSize: 500 * 1024 * 1024, // 500MB max file size
        fieldSize: 500 * 1024 * 1024  // 500MB max field size
    }
});

// Chunk upload endpoint
app.post('/upload-chunk', upload.single('chunk'), (req, res) => {
    try {
        const { chunkIndex, totalChunks, fileName, fileId } = req.body;
        const chunk = req.file;

        if (!chunk) {
            return res.status(400).json({ 
                success: false, 
                message: 'No chunk received' 
            });
        }

        console.log(`Chunk ${chunkIndex + 1}/${totalChunks} received for ${fileName}`);

        // Temporary chunks folder
        const chunksDir = path.join(uploadsDir, 'chunks', fileId);
        if (!fs.existsSync(chunksDir)) {
            fs.mkdirSync(chunksDir, { recursive: true });
        }

        // Chunk file ko save karo
        const chunkFileName = `chunk-${chunkIndex}`;
        const chunkPath = path.join(chunksDir, chunkFileName);
        
        fs.renameSync(chunk.path, chunkPath);

        res.json({
            success: true,
            message: `Chunk ${chunkIndex + 1}/${totalChunks} uploaded successfully`,
            chunkIndex: parseInt(chunkIndex)
        });

    } catch (error) {
        console.error('Chunk upload error:', error);
        res.status(500).json({
            success: false,
            message: 'Chunk upload failed',
            error: error.message
        });
    }
});

// Complete upload endpoint - all chunks merge karo
app.post('/complete-upload', async (req, res) => {
    try {
        const { fileName, totalChunks, fileId } = req.body;

        const chunksDir = path.join(uploadsDir, 'chunks', fileId);
        const outputPath = path.join(uploadsDir, fileName);

        // Check if all chunks are present
        const chunks = fs.readdirSync(chunksDir);
        if (chunks.length !== parseInt(totalChunks)) {
            return res.status(400).json({
                success: false,
                message: `Missing chunks. Expected: ${totalChunks}, Found: ${chunks.length}`
            });
        }

        // Chunks ko merge karo
        const writeStream = fs.createWriteStream(outputPath);

        for (let i = 0; i < totalChunks; i++) {
            const chunkPath = path.join(chunksDir, `chunk-${i}`);
            const chunkBuffer = fs.readFileSync(chunkPath);
            writeStream.write(chunkBuffer);
        }

        writeStream.end();

        writeStream.on('finish', () => {
            // Temporary chunks delete karo
            fs.rmSync(chunksDir, { recursive: true, force: true });
            
            console.log(`File ${fileName} merged successfully!`);
            
            res.json({
                success: true,
                message: 'File uploaded and merged successfully',
                filePath: outputPath,
                fileName: fileName
            });
        });

        writeStream.on('error', (error) => {
            throw error;
        });

    } catch (error) {
        console.error('File merge error:', error);
        res.status(500).json({
            success: false,
            message: 'File merge failed',
            error: error.message
        });
    }
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'Server is running',
        timestamp: new Date().toISOString()
    });
});

// Server start karo
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Upload server running on port ${PORT}`);
    console.log(`Max file size: 500MB`);
    console.log(`Uploads folder: ${uploadsDir}`);
});

module.exports = app;