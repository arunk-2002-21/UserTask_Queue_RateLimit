const express = require('express');
const rateLimit = require('express-rate-limit');
const Bull = require('bull');
const fs = require('fs');

const app = express();
app.use(express.json());

// Rate limiter middleware
const limiter = rateLimit({
    windowMs: 1000, // 1 second window
    max: 1, // limit each user to 1 request per windowMs
    keyGenerator: (req) => req.body.user_id, // rate limit by user ID
    handler: (req, res) => {
        res.status(429).json({ error: 'Too many requests, please try again later.' });
    }
});

// Task processing route
app.post('/task', limiter, async (req, res) => {
    try {
        const user_id = req.body.user_id;
        if (!user_id) {
            return res.status(400).json({ error: 'User ID is required' });
        }

        const userQueue = new Bull(`user-queue-${user_id}`, {
            redis: { port: 6379, host: '127.0.0.1' }
        });

        userQueue.add({ user_id }).catch(err => {
            console.error('Error adding task to queue:', err);
            res.status(500).json({ error: 'Failed to queue task' });
        });

        res.status(202).send('Task queued');

        userQueue.process(async (job) => {
            try {
                await task(job.data.user_id);
                userQueue.on('completed', (job) => {
                    console.log(`Job ${job.id} completed for user ${job.data.user_id}`);
                });
            } catch (err) {
                console.error('Error processing task:', err);
            }
        });

        userQueue.on('error', (err) => {
            console.error('Queue error:', err);
        });

    } catch (err) {
        console.error('Error handling task request:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// Task function
async function task(user_id) {
    try {
        const log = `${user_id}-task completed at-${Date.now()}\n`;
        console.log(log);
        fs.appendFile('task_log.txt', log, (err) => {
            if (err) throw err;
        });
    } catch (err) {
        console.error('Error in task function:', err);
        throw err; // Re-throw to ensure the error is handled by the caller
    }
}

const PORT = 8080;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
