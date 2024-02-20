const { Client } = require('elasticsearch');
const { MongoClient } = require('mongodb');

const mongoURL = 'mongodb+srv://ThanhNguyen:thanhnguyen@springbootrestful.xlacd.mongodb.net';
const client = new MongoClient(mongoURL);

// Create an Elasticsearch client
const elasticClient = new Client({ host: 'https://localhost:9200', httpAuth: 'elastic:6D2nBr9lMXfLHf17P-cY', });

module.exports = {
    client, elasticClient
}