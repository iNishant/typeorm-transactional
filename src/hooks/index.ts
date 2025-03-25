import { EventEmitter } from 'events';

import {
  getHookInContext,
  getTransactionalContext,
  getTransactionalOptions,
  setHookInContext,
} from '../common';
import { StorageDriver } from '../storage/driver/interface';

export const getTransactionalContextHook = () => {
  const context = getTransactionalContext();

  const emitter = getHookInContext(context);
  if (!emitter) {
    throw new Error('No hook manager found in context. Are you using @Transactional()?');
  }

  return emitter;
};

export const runAndTriggerHooks = async (hook: EventEmitter, cb: () => unknown) => {
  try {
    const result = await Promise.resolve(cb());

    // Store the promises returned by commit handlers
    const commitPromises: Promise<unknown>[] = [];

    // promiseCollector is an internal callback emitted with each event.
    // This acts as a channel between the receiver (.once() listener) to send
    // promises back to the emitter.
    const promiseCollector = (promise: Promise<unknown>) => {
      commitPromises.push(promise);
    };

    // Create a promise that will be resolved when all commit handlers are executed
    const commitPromise = new Promise<void>((resolve) => {
      setImmediate(() => {
        // Emit the 'commit' event and collect promises
        hook.emit('commit', promiseCollector);

        // Once all handlers have been called, resolve the promise
        Promise.all(commitPromises).finally(() => {
          // Always clean up after commit handlers, regardless of success/failure
          hook.emit('end', undefined);
          hook.removeAllListeners();
          resolve();
        });
      });
    });

    // Wait for all commit handlers to complete
    await commitPromise;

    return result;
  } catch (err) {
    setImmediate(() => {
      hook.emit('rollback', err);

      hook.emit('end', err);
      hook.removeAllListeners();
    });

    throw err;
  }
};

export const createEventEmitterInNewContext = () => {
  const options = getTransactionalOptions();

  const emitter = new EventEmitter();
  emitter.setMaxListeners(options.maxHookHandlers);
  return emitter;
};

export const runInNewHookContext = async (context: StorageDriver, cb: () => unknown) => {
  const hook = createEventEmitterInNewContext();

  return await context.run(() => {
    setHookInContext(context, hook);

    return runAndTriggerHooks(hook, cb);
  });
};

export const runOnTransactionCommit = (cb: () => void | Promise<unknown>) => {
  getTransactionalContextHook().once(
    'commit',
    (promiseCollector: (promise: Promise<unknown>) => void) => {
      const result = cb();
      // If the original callback returns a promise, we need to collect it
      if (result && typeof result.then === 'function') {
        promiseCollector(result);
      }
      return result;
    },
  );
};

export const runOnTransactionRollback = (cb: (e: Error) => void) => {
  getTransactionalContextHook().once('rollback', cb);
};

export const runOnTransactionComplete = (cb: (e: Error | undefined) => void) => {
  getTransactionalContextHook().once('end', cb);
};
