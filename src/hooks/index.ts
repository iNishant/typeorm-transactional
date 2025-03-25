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
    
    // Create a promise that will be resolved when all commit handlers are executed
    const commitPromise = new Promise<void>((resolve) => {
      setImmediate(() => {
        // Emit the 'commit' event and collect promises
        hook.emit('commit', (promise: Promise<unknown>) => {
          if (promise && typeof promise.then === 'function') {
            commitPromises.push(promise);
          }
        });
        
        // Once all handlers have been called, resolve the promise
        Promise.all(commitPromises)
          .catch(error => console.error('Error in commit handler:', error))
          .finally(() => {
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

export const createEventEmitterInNewContext = (context: StorageDriver) => {
  const options = getTransactionalOptions();

  const emitter = new EventEmitter();
  emitter.setMaxListeners(options.maxHookHandlers);
  return emitter;
};

export const runInNewHookContext = async (context: StorageDriver, cb: () => unknown) => {
  const hook = createEventEmitterInNewContext(context);

  return await context.run(() => {
    setHookInContext(context, hook);

    return runAndTriggerHooks(hook, cb);
  });
};

export const runOnTransactionCommit = (cb: () => void | Promise<unknown>) => {
  getTransactionalContextHook().once('commit', (collectPromise?: (promise: Promise<unknown>) => void) => {
    const result = cb();
    if (result && typeof result.then === 'function' && collectPromise) {
      collectPromise(result);
    }
    return result;
  });
};

export const runOnTransactionRollback = (cb: (e: Error) => void) => {
  getTransactionalContextHook().once('rollback', cb);
};

export const runOnTransactionComplete = (cb: (e: Error | undefined) => void) => {
  getTransactionalContextHook().once('end', cb);
};
