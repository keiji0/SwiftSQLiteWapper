//
//  Statement.swift
//  
//
//  Created by keiji0 on 2020/12/26.
//

import Foundation
import SQLite3

public final class Statement {
    
    var handle: OpaquePointer?
    unowned var connection: Connection
    
    public init(_ connection: Connection, sql: String) throws {
        self.connection = connection
        try connection.call {
            sqlite3_prepare_v2(connection.handle, sql, -1, &handle, nil)
        }
    }
    
    deinit {
        sqlite3_finalize(handle)
    }
    
    @discardableResult
    public func step() throws -> QueryResult {
        assert(handle != nil)
        return try connection.call {
            sqlite3_step(handle)
        }
    }

    public func reset() throws {
        assert(handle != nil)
        try connection.call {
            sqlite3_reset(handle)
        }
    }

    public func isNull(_ index: Int) -> Bool {
        assert(handle != nil)
        return sqlite3_column_type(handle, Int32(index)) == SQLITE_NULL
    }
    
    @discardableResult
    public func bind(_ params: some Collection<StatementParameter>) throws -> Statement {
        try params.enumerated().forEach { index, value in
            try value.bind(from: self, index: index + 1)
        }
        return self
    }
    
    public func column<T: StatementParameter>(_ type: T.Type, _ index: Int) -> T {
        return T(from: self, index: index)
    }
    
    public func column<T: StatementParameter>(_ index: Int) -> T {
        return T(from: self, index: index)
    }
    
    public func fetchRow<T>(_ block: (Statement) -> T?)  throws -> T? {
        assert(handle != nil)
        guard try step() == .row else { return nil }
        return block(self)
    }
    
    public func fetchRow(_ block: (Statement) -> Void)  throws {
        assert(handle != nil)
        guard try step() == .row else { return }
        block(self)
    }
    
    public func fetchRows<T>(_ block: (Statement) -> T) throws -> [T] {
        assert(handle != nil)
        var res: [T] = []
        while try step() == .row {
            res.append(block(self))
        }
        return res
    }
}
