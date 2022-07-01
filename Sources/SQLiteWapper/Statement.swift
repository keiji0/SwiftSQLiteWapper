//
//  Statement.swift
//  
//
//  Created by keiji0 on 2020/12/26.
//

import Foundation
import SQLite3

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// SQLのステートメント
public final class Statement {
    
    public init(_ connection: Connection, sql: String) throws {
        self.connection = connection
        try connection.call {
            sqlite3_prepare_v2(connection.handle, sql, -1, &handle, nil)
        }
    }
    
    deinit {
        sqlite3_finalize(handle)
    }
    
    /// ステートメントに値を設定する
    public func bind(_ values: some Collection<Value>) throws {
        assert(handle != nil)
        try reset()
        try values.enumerated().forEach { index, value in
            try value.bind(Binder(self.connection, self.handle!, index + 1))
        }
    }
    
    /// 評価後の行を取得する。なければnilが戻る
    public func fetchRow() throws -> Row? {
        assert(handle != nil)
        guard try step() == .row else {
            return nil
        }
        return .init(handle!)
    }
    
    /// 評価後の行のシーケンスを取得する
    public func fetchRows() throws -> some Sequence<Row> {
        assert(handle != nil)
        return RowSequence(self)
    }
    
    // MARK: - Internal

    func step() throws -> QueryResult {
        try connection.call { sqlite3_step(handle) }
    }
    
    func reset() throws {
        assert(handle != nil)
        try connection.call { sqlite3_reset(handle) }
        try connection.call { sqlite3_clear_bindings(handle) }
    }
    
    // MARK: - Private
    
    private var handle: OpaquePointer?
    private unowned let connection: Connection
    
    private struct RowSequence: Sequence, IteratorProtocol {
        private unowned let statement: Statement
        init(_ statement: Statement) {
            self.statement = statement
        }
        
        mutating func next() -> Row? {
            try? statement.fetchRow()
        }
    }
}
