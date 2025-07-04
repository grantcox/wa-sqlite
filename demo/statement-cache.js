export class StatementCache {
    constructor(capacity, evictionCallback) {
        this.capacity = capacity;
        this.evictionCallback = evictionCallback;
        this.cache = new Map();
    }

    // Get value and mark as recently used
    get(sql) {
        if (!this.cache.has(sql)) {
            return;
        }

        // Remove and re-add to make it the most recently used
        const stmt = this.cache.get(sql);
        this.cache.delete(sql);
        this.cache.set(sql, stmt);

        return stmt;
    }

    // Add or update an entry
    set(sql, stmt) {
        // If key exists, delete it first to update its position
        if (this.cache.has(sql)) {
            this.cache.delete(sql);
        } else if (this.cache.size >= this.capacity) {
            // Map.keys().next() gives us the oldest key (least recently used)
            const oldestKey = this.cache.keys().next().value;
            const evictStmt = this.cache.get(oldestKey);
            this.cache.delete(oldestKey);
            this.evictionCallback(evictStmt);
        }
        this.cache.set(sql, stmt);
    }

    flush() {
        this.cache.forEach((stmt, sql) => {
            this.evictionCallback(stmt);
        });
        this.cache.clear();
    }
}
