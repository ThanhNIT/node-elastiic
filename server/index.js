const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { readPDF, processDocuments, clearIndex, processPDFs } = require('./FileService');
const { elasticClient } = require('./config');

const app = express();
const port = 3001;

// MongoDB connection URL

// Middleware
app.use(bodyParser.json());
app.use(cors())

// Routes
app.get('/search', async (req, res) => {
  const { query } = req.query;
  
  try {
    const result = await searchDocuments(query);
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/indexing', async (req, res) => {  
  try {
    const { path } = req.query
    processPDFs(path);
    res.json('Processing');
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/clear-index', async (req, res) => {  
  try {
    clearIndex();
    res.json('Processing');
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Function to search documents in Elasticsearch
async function searchDocuments(query) {
  const { hits : { hits } } = await elasticClient.search({
    index: 'documents_index',
    body: {
      query: {
        match: {
          content: `*${query}*`
        }
      },
      size: 10000,
      "_source": ["docId"]
    }
  });
  
  return hits;
}

// Start the server
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});