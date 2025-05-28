import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { FastifyAdapter } from '@bull-board/fastify';
import fastify, { FastifyInstance, FastifyRequest } from 'fastify';
import { Server, IncomingMessage, ServerResponse } from 'http';
import { env } from './env';

import { createQueue, setupQueueProcessor } from './queue';

const run = async () => {
  const webhookQueue = createQueue('WebhookQueue');
  await setupQueueProcessor(webhookQueue.name);

  const server: FastifyInstance<Server, IncomingMessage, ServerResponse> =
    fastify();

  const serverAdapter = new FastifyAdapter();
  createBullBoard({
    queues: [new BullMQAdapter(webhookQueue)],
    serverAdapter,
  });
  serverAdapter.setBasePath('/');
  server.register(serverAdapter.registerPlugin(), {
    prefix: '/',
    basePath: '/',
  });

  // High priority webhook endpoint
  server.post('/webhook/high-priority', async (req: FastifyRequest, reply) => {
    try {
      const webhookData = req.body;

      // Add job with high priority (100)
      const job = await webhookQueue.add(
        'webhook-high',
        { data: webhookData },
        { priority: 100 }
      );

      // Wait for job completion (max 2 minutes)
      const result = await job.waitUntilFinished(undefined, 120000);

      reply.send(result);
    } catch (error) {
      console.error('High priority webhook error:', error);
      reply.status(500).send({
        error: 'Failed to process webhook',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  // Low priority webhook endpoint
  server.post('/webhook/low-priority', async (req: FastifyRequest, reply) => {
    try {
      const webhookData = req.body;

      // Add job with low priority (1)
      const job = await webhookQueue.add(
        'webhook-low',
        { data: webhookData },
        { priority: 1 }
      );

      // Wait for job completion (max 2 minutes)
      const result = await job.waitUntilFinished(undefined, 120000);

      reply.send(result);
    } catch (error) {
      console.error('Low priority webhook error:', error);
      reply.status(500).send({
        error: 'Failed to process webhook',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  await server.listen({ port: env.PORT, host: '0.0.0.0' });
  console.log(`Server running on port ${env.PORT}`);
  console.log(`Webhook endpoints available:`);
  console.log(`- High priority: https://${env.RAILWAY_STATIC_URL}/webhook/high-priority`);
  console.log(`- Low priority: https://${env.RAILWAY_STATIC_URL}/webhook/low-priority`);
  console.log(`- Dashboard: https://${env.RAILWAY_STATIC_URL}/`);
};

run().catch((e) => {
  console.error(e);
  process.exit(1);
});