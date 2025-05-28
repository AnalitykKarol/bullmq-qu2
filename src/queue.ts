import { ConnectionOptions, Queue, QueueScheduler, Worker } from 'bullmq';

import { env } from './env';

const connection: ConnectionOptions = {
  host: env.REDISHOST,
  port: env.REDISPORT,
  username: env.REDISUSER,
  password: env.REDISPASSWORD,
};

export const createQueue = (name: string) => new Queue(name, { connection });

export const setupQueueProcessor = async (queueName: string) => {
  const queueScheduler = new QueueScheduler(queueName, {
    connection,
  });
  await queueScheduler.waitUntilReady();

  new Worker(
    queueName,
    async (job) => {
      try {
        // Log start of processing
        await job.log(`Processing webhook job ${job.id} with priority ${job.opts.priority}`);
        await job.updateProgress(10);

        // Prepare the data to send to n8n
        const webhookData = job.data.data;

        await job.log(`Sending data to n8n: ${env.N8N_WEBHOOK_URL}`);
        await job.updateProgress(30);

        // Create abort controller for timeout (2 minutes = 120000ms)
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 120000);

        try {
          // Make HTTP POST request to n8n
          const response = await fetch(env.N8N_WEBHOOK_URL, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify(webhookData),
            signal: controller.signal,
          });

          clearTimeout(timeoutId);
          await job.updateProgress(70);

          if (!response.ok) {
            throw new Error(`n8n responded with status ${response.status}: ${response.statusText}`);
          }

          await job.log(`Received response from n8n with status ${response.status}`);
          await job.updateProgress(90);

          // Parse response from n8n
          const n8nResponse = await response.json();

          await job.updateProgress(100);
          await job.log(`Successfully processed webhook job ${job.id}`);

          // Return the response from n8n
          return n8nResponse;

        } catch (fetchError) {
          clearTimeout(timeoutId);

          if (fetchError instanceof Error) {
            if (fetchError.name === 'AbortError') {
              throw new Error('Request to n8n timed out after 2 minutes');
            }
            throw new Error(`Failed to call n8n webhook: ${fetchError.message}`);
          }

          throw new Error('Unknown error occurred while calling n8n webhook');
        }

      } catch (error) {
        await job.log(`Error processing webhook job ${job.id}: ${error instanceof Error ? error.message : 'Unknown error'}`);
        throw error;
      }
    },
    {
      connection,
      concurrency: 10, // Process up to 10 jobs simultaneously
    }
  );
};