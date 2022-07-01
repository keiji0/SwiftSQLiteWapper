//
//  Binder.swift
//  
//  
//  Created by keiji0 on 2022/07/01
//  
//

import Foundation
import SQLite3

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// ステートメントに値を束縛してくれるクラス
public struct Binder {
    
    init(_ connection: Connection, _ handle: OpaquePointer, _ index: Int) {
        self.connection = connection
        self.index = index
        self.handle = handle
    }
    
    public func bind(_ value: Int32) throws {
        try connection.call {
            sqlite3_bind_int(handle, Int32(index), value)
        }
    }
    
    public func bind(_ value: Int64) throws {
        try connection.call {
            sqlite3_bind_int64(handle, Int32(index), value)
        }
    }
    
    public func bind(_ value: Double) throws {
        try connection.call {
            sqlite3_bind_double(handle, Int32(index), value)
        }
    }
    
    public func bind(_ value: String) throws {
        try connection.call {
            sqlite3_bind_text(handle, Int32(index), value, -1, SQLITE_TRANSIENT)
        }
    }
    
    public func bind(_ value: Data) throws {
        try connection.call {
            value.withUnsafeBytes { pointer in
                sqlite3_bind_blob(handle, Int32(index), pointer.baseAddress, Int32(value.count), SQLITE_TRANSIENT)
            }
        }
    }
    
    public func bindNull(_ index: Int) throws {
        try connection.call {
            sqlite3_bind_null(handle, Int32(index))
        }
    }
    
    // MARK: - Internal
    
    private let index: Int
    private unowned let connection: Connection
    private let handle: OpaquePointer
}
