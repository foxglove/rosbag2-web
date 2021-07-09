import {
  Filelike,
  MessageReadOptions,
  MessageRow,
  RawMessage,
  RawMessageIterator,
  SqliteDb,
  TopicDefinition,
  parseQosProfiles,
} from "@foxglove/rosbag2";
import { Time, fromNanoSec, toNanoSec } from "@foxglove/rostime";
import initSqlJs, { Database, Statement } from "sql.js";

type DbContext = {
  db: Database;
  idToTopic: Map<bigint, TopicDefinition>;
  topicNameToId: Map<string, bigint>;
};

type TopicRowArray = [
  id: number,
  name: string,
  type: string,
  serialization_format: string,
  offered_qos_profiles: string,
];

type MessageRowArray = [topic_id: number, timestamp: string, data: Uint8Array];

export class SqliteSqljs implements SqliteDb {
  readonly file: Readonly<Filelike>;
  private locateSqlJsWasm?: (file: string) => string;
  private context?: DbContext;

  constructor(file: Filelike, locateSqlJsWasm?: (file: string) => string) {
    this.file = file;
    this.locateSqlJsWasm = locateSqlJsWasm;
  }

  async open(): Promise<void> {
    const SQL = await initSqlJs({ locateFile: this.locateSqlJsWasm });

    const data = await this.file.read();
    const db = new SQL.Database(new Uint8Array(data));

    // Retrieve all of the topics
    const idToTopic = new Map<bigint, TopicDefinition>();
    const topicNameToId = new Map<string, bigint>();
    const topicRows = (db.exec(
      "select id,name,type,serialization_format,offered_qos_profiles from topics",
    )[0]?.values ?? []) as TopicRowArray[];
    for (const row of topicRows) {
      const [id, name, type, serializationFormat, qosProfilesStr] = row;
      const offeredQosProfiles = parseQosProfiles(qosProfilesStr);
      const topic = { name, type, serializationFormat, offeredQosProfiles };
      const bigintId = BigInt(id);
      idToTopic.set(bigintId, topic);
      topicNameToId.set(name, bigintId);
    }

    this.context = { db, idToTopic, topicNameToId };
    return Promise.resolve();
  }

  async close(): Promise<void> {
    if (this.context != undefined) {
      this.context.db.close();
      this.context = undefined;
    }
    await this.file.close();
  }

  readTopics(): Promise<TopicDefinition[]> {
    if (this.context == undefined) {
      throw new Error(`Call open() before reading topics`);
    }
    return Promise.resolve(Array.from(this.context.idToTopic.values()));
  }

  readMessages(opts: MessageReadOptions = {}): AsyncIterableIterator<RawMessage> {
    if (this.context == undefined) {
      throw new Error(`Call open() before reading messages`);
    }
    const db = this.context.db;
    const topicNameToId = this.context.topicNameToId;

    // Build a SQL query and bind parameters
    let args: (string | number)[] = [];
    let query = `select topic_id,cast(timestamp as TEXT) as timestamp,data from messages`;
    if (opts.startTime != undefined) {
      query += ` where timestamp >= cast(? as INTEGER)`;
      args.push(toNanoSec(opts.startTime).toString());
    }
    if (opts.endTime != undefined) {
      if (args.length === 0) {
        query += ` where timestamp < cast(? as INTEGER)`;
      } else {
        query += ` and timestamp < cast(? as INTEGER)`;
      }
      args.push(toNanoSec(opts.endTime).toString());
    }
    if (opts.topics != undefined) {
      // Map topics to topic_ids
      const topicIds: number[] = [];
      for (const topicName of opts.topics) {
        const topicId = topicNameToId.get(topicName);
        if (topicId != undefined) {
          topicIds.push(Number(topicId));
        }
      }

      if (topicIds.length === 0) {
        if (args.length === 0) {
          query += ` where topic_id = NULL`;
        } else {
          query += ` and topic_id = NULL`;
        }
      } else if (topicIds.length === 1) {
        if (args.length === 0) {
          query += ` where topic_id = ?`;
        } else {
          query += ` and topic_id = ?`;
        }
        args.push(topicIds[0]!);
      } else {
        if (args.length === 0) {
          query += ` where topic_id in (${topicIds.map(() => "?").join(",")})`;
        } else {
          query += ` and topic_id in (${topicIds.map(() => "?").join(",")})`;
        }
        args = args.concat(topicIds);
      }
    }

    const statement = db.prepare(query, args);
    const dbIterator = new SqlJsMessageRowIterator(statement);
    return new RawMessageIterator(dbIterator, this.context.idToTopic);
  }

  timeRange(): Promise<[min: Time, max: Time]> {
    if (this.context == undefined) {
      throw new Error(`Call open() before retrieving the time range`);
    }
    const db = this.context.db;

    const res = db.exec(
      "select cast(min(timestamp) as TEXT), cast(max(timestamp) as TEXT) from messages",
    )[0]?.values[0] ?? ["0", "0"];
    const [minNsec, maxNsec] = res as [string, string];
    return Promise.resolve([fromNanoSec(BigInt(minNsec)), fromNanoSec(BigInt(maxNsec))]);
  }

  messageCounts(): Promise<Map<string, number>> {
    if (this.context == undefined) {
      throw new Error(`Call open() before retrieving message counts`);
    }
    const db = this.context.db;

    const rows =
      db.exec(`
    select topics.name,count(*)
    from messages
    inner join topics on messages.topic_id = topics.id
    group by topics.id`)[0]?.values ?? ([] as [string, number][]);
    const counts = new Map<string, number>();
    for (const [topicName, count] of rows) {
      counts.set(topicName as string, count as number);
    }
    return Promise.resolve(counts);
  }
}

class SqlJsMessageRowIterator implements IterableIterator<MessageRow> {
  statement: Statement;

  constructor(statement: Statement) {
    this.statement = statement;
  }

  [Symbol.iterator](): IterableIterator<MessageRow> {
    return this;
  }

  next(): IteratorResult<MessageRow> {
    if (!this.statement.step()) {
      return { value: undefined, done: true };
    }

    const [topic_id, timestamp, data] = this.statement.get() as MessageRowArray;
    return {
      value: { topic_id: BigInt(topic_id), timestamp: BigInt(timestamp), data },
      done: false,
    };
  }

  return(): IteratorResult<MessageRow> {
    this.statement.freemem();
    this.statement.free();
    return { value: undefined, done: true };
  }
}
