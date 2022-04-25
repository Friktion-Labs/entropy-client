/**
This will probably move to its own repo at some point but easier to keep it here for now
 */
import * as os from 'os';
import * as fs from 'fs';
import { EntropyClient } from './client';
import {
  Account,
  Commitment,
  Connection,
  PublicKey,
  Transaction,
} from '@solana/web3.js';
import { getMultipleAccounts, zeroKey } from './utils';
import configFile from './ids.json';
import { Cluster, Config } from './config';
import {
  makeCachePerpMarketsInstruction,
  makeCachePricesInstruction,
  makeCacheRootBankInstruction,
  makeUpdateFundingInstruction,
  makeUpdateRootBankInstruction,
} from './instruction';
import BN from 'bn.js';
import { PerpEventQueueLayout } from './layout';
import { EntropyGroup, PerpMarket, promiseUndef } from '.';
import PerpEventQueue from './PerpEventQueue';
import { PROGRAM_LAYOUT_VERSIONS } from '@project-serum/serum/lib/tokens_and_markets';
import yargs from 'yargs/yargs';
import { hideBin } from 'yargs/helpers';
import { Options, PositionalOptions } from 'yargs';
import { should } from 'chai';

require('dotenv').config({ path: '.env' });

let lastRootBankCacheUpdate = 0;
const groupName = process.env.GROUP || 'mainnet.2';
const updateCacheInterval = parseInt(
  process.env.UPDATE_CACHE_INTERVAL || '20000',
);
const updateRootBankCacheInterval = parseInt(
  process.env.UPDATE_ROOT_BANK_CACHE_INTERVAL || '20000',
);
const processKeeperInterval = parseInt(
  process.env.PROCESS_KEEPER_INTERVAL || '30000',
);
const consumeEventsInterval = parseInt(
  process.env.CONSUME_EVENTS_INTERVAL || '5000',
);
const maxUniqueAccounts = parseInt(process.env.MAX_UNIQUE_ACCOUNTS || '10');
const consumeEventsLimit = new BN(process.env.CONSUME_EVENTS_LIMIT || '10');
const consumeEvents = process.env.CONSUME_EVENTS
  ? process.env.CONSUME_EVENTS === 'true'
  : true;
const cluster = (process.env.CLUSTER || 'mainnet') as Cluster;
const config = new Config(configFile);
const groupIds = config.getGroup(cluster, groupName);

if (!groupIds) {
  throw new Error(`Group ${groupName} not found`);
}
const entropyProgramId = groupIds.entropyProgramId;
console.log("PROGRAM ID: ", entropyProgramId.toString())
const entropyGroupKey = groupIds.publicKey;
const payerJsonFile =  fs.readFileSync(process.env.KEYPAIR || (os.homedir() + '/.config/solana/entropy-mainnet-authority.json'), 'utf-8');
const payer = new Account(
  JSON.parse(
    payerJsonFile
  ),
);
const RPC_ENDPOINT = (process.env.RPC_ENDPOINT || config.cluster_urls[cluster])
console.log("RPC_ENDPOINT USED", RPC_ENDPOINT);
const connection = new Connection(
  RPC_ENDPOINT,
  'confirmed' as Commitment,
);
console.log("DEVNET RPC: ", process.env.DEVNET_ENDPOINT_URL)
const client = new EntropyClient(connection, entropyProgramId);

export async function runKeeper(shouldRun=0) {
  console.log("shouldRun: ", shouldRun);
  if (shouldRun != 1) {
    return;
  }
  if (!groupIds) {
    throw new Error(`Group ${groupName} not found`);
  }
  const entropyGroup = await client.getEntropyGroup(entropyGroupKey);
  const perpMarkets = await Promise.all(
    groupIds.perpMarkets.map((m) => {
      return entropyGroup.loadPerpMarket(
        connection,
        m.marketIndex,
        m.baseDecimals,
        m.quoteDecimals,
      );
    }),
  );

  processUpdateCache(entropyGroup);
  processKeeperTransactions(entropyGroup, perpMarkets);

  if (consumeEvents) {
    processConsumeEvents(entropyGroup, perpMarkets);
  }
}
console.time('processUpdateCache');

async function processUpdateCache(entropyGroup: EntropyGroup) {
  console.timeEnd('processUpdateCache');

  try {
    const batchSize = 8;
    let promises: Promise<string>[] = [];
    const rootBanks = entropyGroup.tokens
      .map((t) => t.rootBank)
      .filter((t) => !t.equals(zeroKey));
    const oracles = entropyGroup.oracles.filter((o) => !o.equals(zeroKey));
    const perpMarkets = entropyGroup.perpMarkets
      .filter((pm) => !pm.isEmpty())
      .map((pm) => pm.perpMarket);
    const nowTs = Date.now();
    let shouldUpdateRootBankCache = false;
    if (nowTs - lastRootBankCacheUpdate > updateRootBankCacheInterval) {
      shouldUpdateRootBankCache = true;
      lastRootBankCacheUpdate = nowTs;
    }
    for (let i = 0; i < rootBanks.length / batchSize; i++) {
      const startIndex = i * batchSize;
      const endIndex = i * batchSize + batchSize;
      const cacheTransaction = new Transaction();
      if (shouldUpdateRootBankCache) {
        cacheTransaction.add(
          makeCacheRootBankInstruction(
            entropyProgramId,
            entropyGroup.publicKey,
            entropyGroup.entropyCache,
            rootBanks.slice(startIndex, endIndex),
          ),
        );
      }

      if (cacheTransaction.instructions.length > 0) {
        promises.push(client.sendTransaction(cacheTransaction, payer, []));
      }
    }

    Promise.all(promises).catch((err) => {
      console.error('Error updating cache', err);
    });

    promises = [];
    for (let i = 0; i < oracles.length / batchSize; i++) {
      const startIndex = i * batchSize;
      const endIndex = i * batchSize + batchSize;
      const cacheTransaction = new Transaction();

      console.log('oracles: ', oracles.toString());

      cacheTransaction.add(
        makeCachePricesInstruction(
          entropyProgramId,
          entropyGroup.publicKey,
          entropyGroup.entropyCache,
          oracles.slice(startIndex, endIndex),
        ),
      );

      cacheTransaction.add(
        makeCachePerpMarketsInstruction(
          entropyProgramId,
          entropyGroup.publicKey,
          entropyGroup.entropyCache,
          perpMarkets.slice(startIndex, endIndex),
        ),
      );
      if (cacheTransaction.instructions.length > 0) {
        promises.push(client.sendTransaction(cacheTransaction, payer, []));
      }
    }

    Promise.all(promises).catch((err) => {
      console.error('Error updating cache', err);
    });

  } finally {
    console.time('processUpdateCache');
    setTimeout(processUpdateCache, updateCacheInterval, entropyGroup);
  }
}

async function processConsumeEvents(
  entropyGroup: EntropyGroup,
  perpMarkets: PerpMarket[],
) {
  try {
    const eventQueuePks = perpMarkets.map((mkt) => mkt.eventQueue);
    const eventQueueAccts = await getMultipleAccounts(
      connection,
      eventQueuePks,
    );

    const perpMktAndEventQueue = eventQueueAccts.map(
      ({ publicKey, accountInfo }) => {
        const parsed = PerpEventQueueLayout.decode(accountInfo?.data);
        const eventQueue = new PerpEventQueue(parsed);
        const perpMarket = perpMarkets.find((mkt) =>
          mkt.eventQueue.equals(publicKey),
        );
        if (!perpMarket) {
          throw new Error('PerpMarket not found');
        }
        return { perpMarket, eventQueue };
      },
    );

    const promises: Promise<string | void>[] = perpMktAndEventQueue.map(
      ({ perpMarket, eventQueue }) => {
        const events = eventQueue.getUnconsumedEvents();
        if (events.length === 0) {
          // console.log('No events to consume');
          return promiseUndef();
        }

        const accounts: Set<string> = new Set();
        for (const event of events) {
          if (event.fill) {
            accounts.add(event.fill.maker.toBase58());
            accounts.add(event.fill.taker.toBase58());
          } else if (event.out) {
            accounts.add(event.out.owner.toBase58());
          }

          // Limit unique accounts to first 20 or 21
          if (accounts.size >= maxUniqueAccounts) {
            break;
          }
        }

        return client
          .consumeEvents(
            entropyGroup,
            perpMarket,
            Array.from(accounts)
              .map((s) => new PublicKey(s))
              .sort(),
            payer,
            consumeEventsLimit,
          )
          .then(() => {
            console.log(
              `Consumed up to ${
                events.length
              } events ${perpMarket.publicKey.toBase58()}`,
            );
            console.log(
              'EVENTS:',
              events.map((e) => e?.fill?.seqNum.toString()),
            );
          })
          .catch((err) => {
            console.error('Error consuming events', err);
          });
      },
    );

    Promise.all(promises);
  } finally {
    setTimeout(
      processConsumeEvents,
      consumeEventsInterval,
      entropyGroup,
      perpMarkets,
    );
  }
}

async function processKeeperTransactions(
  entropyGroup: EntropyGroup,
  perpMarkets: PerpMarket[],
) {
  try {
    if (!groupIds) {
      throw new Error(`Group ${groupName} not found`);
    }
    console.log('processKeeperTransactions');
    const batchSize = 8;
    const promises: Promise<string>[] = [];

    const filteredPerpMarkets = perpMarkets.filter(
      (pm) => !pm.publicKey.equals(zeroKey),
    );

    for (let i = 0; i < groupIds.tokens.length / batchSize; i++) {
      const startIndex = i * batchSize;
      const endIndex = i * batchSize + batchSize;

      const updateRootBankTransaction = new Transaction();
      groupIds.tokens.slice(startIndex, endIndex).forEach((token) => {
        updateRootBankTransaction.add(
          makeUpdateRootBankInstruction(
            entropyProgramId,
            entropyGroup.publicKey,
            entropyGroup.entropyCache,
            token.rootKey,
            token.nodeKeys,
          ),
        );
      });

      const updateFundingTransaction = new Transaction();
      filteredPerpMarkets.slice(startIndex, endIndex).forEach((market) => {
        if (market) {
          updateFundingTransaction.add(
            makeUpdateFundingInstruction(
              entropyProgramId,
              entropyGroup.publicKey,
              entropyGroup.entropyCache,
              market.publicKey,
              market.bids,
              market.asks,
            ),
          );
        }
      });

      if (updateRootBankTransaction.instructions.length > 0) {
        promises.push(
          client.sendTransaction(updateRootBankTransaction, payer, []),
        );
      }
      if (updateFundingTransaction.instructions.length > 0) {
        promises.push(
          client.sendTransaction(updateFundingTransaction, payer, []),
        );
      }
    }

    Promise.all(promises).catch((err) => {
      console.error('Error processing keeper instructions', err);
    });
  } finally {
    setTimeout(
      processKeeperTransactions,
      processKeeperInterval,
      entropyGroup,
      perpMarkets,
    );
  }
}

yargs(hideBin(process.argv)).command(
  'run',
  'runs the keeper',
  (y) => {
    return;
  },
  async (args) => {
    console.log("Running keeper via args");
    runKeeper(1);
  },
).argv;
