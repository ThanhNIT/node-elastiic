const fs = require('fs');
const pdf = require('pdf-parse');
const mammoth = require('mammoth');
const { client, elasticClient } = require('./config');

// Database Name
const dbName = 'epost';
// Collection Name
const collectionName = 'emails';

const indexName = 'documents_index';

// Function to connect to MongoDB, process documents, and index to Elasticsearch
async function processDocuments() {
    try {
        // Connect to the MongoDB server
        await client.connect();
        console.log('Connected to MongoDB');

        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        // Count all documents in the collection
        const totalDocuments = await collection.countDocuments();
        console.log('Total documents to process:', totalDocuments);

        // Find all documents in the collection
        const cursor = collection.find();

        let processedDocuments = 0;

        // Iterate over each document
        await cursor.forEach(async (doc) => {
            try {
                const fileURL = doc.fileURL;

                // Process the file based on its extension
                let content;
                if (fileURL.endsWith('.pdf')) {
                    content = await readPDF(fileURL);
                } else if (fileURL.endsWith('.docx')) {
                    content = await readDOCX(fileURL);
                } else {
                    console.log('Unsupported file format:', fileURL);
                    return;
                }

                // Index content to Elasticsearch
                await indexContent(doc._id, doc.fileURL, content);

                // Increment processed documents count
                processedDocuments++;
                console.log(`Processed ${processedDocuments} out of ${totalDocuments} documents`);
            } catch (error) {
                console.error('Error processing document:', error);
            }
        });

        console.log('Processing completed');
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
    } finally {
        // Close the MongoDB connection
        await client.close();
        console.log('Disconnected from MongoDB');
    }
}


async function processPDFs(folderPath) {
    try {
        // Get list of PDF files in the folder
        const pdfFiles = fs.readdirSync(folderPath).filter(file => file.endsWith('.pdf'));
        const totalFiles = pdfFiles.length;
        console.log('Total PDF files to process:', totalFiles);

        let processedFiles = 0;

        // Iterate over each PDF file
        for (const file of pdfFiles) {
            try {
                const filePath = path.join(folderPath, file);

                // Read content from PDF file
                const content = await readPDF(filePath);

                // Index content to Elasticsearch
                await indexContent(file, filePath, content);

                // Increment processed files count
                processedFiles++;
                console.log(`Processed ${processedFiles} out of ${totalFiles} files`);
            } catch (error) {
                console.error(`Error processing file ${file}:`, error);
            }
        }

        console.log('Processing completed');
    } catch (error) {
        console.error('Error reading PDF files:', error);
    }
}

// Function to clear the Elasticsearch index
async function clearIndex() {
    try {
        await elasticClient.deleteByQuery({
            index: indexName,
            body: {
                query: {
                    match_all: {}
                }
            }
        });
        console.log('Index cleared');
    } catch (error) {
        console.error('Error clearing index:', error);
    }
}

// Function to read content from a PDF file
async function readPDF(filePath) {
    try {
        const data = await fs.promises.readFile(filePath);
        const pdfContent = await pdf(data);
        return pdfContent.text;
    } catch (error) {
        console.error('Error reading PDF:', error);
        return null;
    }
}



// Function to read content from a DOCX file
async function readDOCX(filePath) {
    try {
        const { value } = await mammoth.extractRawText({ path: filePath });
        return value;
    } catch (error) {
        console.error('Error reading DOCX:', error);
        return null;
    }
}

// Function to index content to Elasticsearch
async function indexContent(docId, docUrl, content) {
    try {
        await elasticClient.index({
            index: indexName,
            id: docId,
            body: { docUrl, content }
        });
        console.log('Document indexed:', docId);
    } catch (error) {
        console.error('Error indexing document:', error);
    }
}

// Call the function to start processing documents
// processDocuments();

module.exports = { clearIndex, processDocuments, processPDFs}
