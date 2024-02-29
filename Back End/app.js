const express = require('express');
const movieRoutes = require('./Routes/routes');
const { connectToDatabase, closeDatabaseConnection } = require('./Controllers/db'); // Update the path accordingly
const app = express();
const PORT = 3000;
const cors = require('cors');

app.use(cors());
app.use(express.json());

// Use routes
app.use('/api', movieRoutes);

// Connect to SQL Server and start the server
connectToDatabase().then(() => {
  app.listen(PORT, () => {
    console.log(`Server is running at http://localhost:${PORT}`);
  });
});

// Close the database connection when the Node.js process exits
process.on('exit', () => {
  closeDatabaseConnection();
});


// Handle process termination signals (e.g., Ctrl+C in the terminal)
process.on('SIGINT', () => {
  closeDatabaseConnection();
  process.exit();
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  closeDatabaseConnection();
  process.exit(1);
});
