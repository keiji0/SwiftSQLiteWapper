//
//  StatementParameter.swift
//  
//
//  Created by keiji0 on 2020/12/27.
//

import Foundation
import SQLite3

private let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// ステートメントで扱うパラメータープロトコル
/// アプリ側で拡張したい場合はこのプロトコルの準拠する
public protocol StatementParameter {
    /// ステートメントから値を取得
    init(from statement: Statement, index: Int)
    /// パラメーターをステートメント紐付ける
    func bind(from statement: Statement, index: Int) throws
}

extension Int32 : StatementParameter {
    public init(from statement: Statement, index: Int) {
        assert(sqlite3_column_type(statement.handle, Int32(index)) == SQLITE_INTEGER)
        self = sqlite3_column_int(statement.handle, Int32(index))
    }
    
    public func bind(from statement: Statement, index: Int) throws {
        try statement.connection.call {
            sqlite3_bind_int(statement.handle, Int32(index), Int32(self))
        }
    }
}

extension Int64 : StatementParameter {
    public init(from statement: Statement, index: Int) {
        assert(sqlite3_column_type(statement.handle, Int32(index)) == SQLITE_INTEGER)
        self = sqlite3_column_int64(statement.handle, Int32(index))
    }

    public func bind(from statement: Statement, index: Int) throws {
        try statement.connection.call {
            sqlite3_bind_int64(statement.handle, Int32(index), Int64(self))
        }
    }
}

extension Int : StatementParameter {
    public init(from statement: Statement, index: Int) {
        self = Int(Int64(from: statement, index: index))
    }

    public func bind(from statement: Statement, index: Int) throws {
        try Int64(self).bind(from: statement, index: index)
    }
}

extension Int16 : StatementParameter {
    public init(from statement: Statement, index: Int) {
        self = Int16(Int32(from: statement, index: index))
    }

    public func bind(from statement: Statement, index: Int) throws {
        try Int32(self).bind(from: statement, index: index)
    }
}

extension Double : StatementParameter {
    public init(from statement: Statement, index: Int) {
        assert(sqlite3_column_type(statement.handle, Int32(index)) == SQLITE_FLOAT)
        self = sqlite3_column_double(statement.handle, Int32(index))
    }

    public func bind(from statement: Statement, index: Int) throws {
        try statement.connection.call {
            sqlite3_bind_double(statement.handle, Int32(index), self)
        }
    }
}

extension Bool : StatementParameter {
    public init(from statement: Statement, index: Int) {
        self = Int32(from: statement, index: index) != 0
    }

    public func bind(from statement: Statement, index: Int) throws {
        try Int32(self ? 1 : 0).bind(from: statement, index: index)
    }
}

extension String : StatementParameter {
    public init(from statement: Statement, index: Int) {
        assert(sqlite3_column_type(statement.handle, Int32(index)) == SQLITE_TEXT)
        self = String(cString: UnsafePointer(sqlite3_column_text(statement.handle, Int32(index))))
    }

    public func bind(from statement: Statement, index: Int) throws {
        try statement.connection.call {
            sqlite3_bind_text(statement.handle, Int32(index), self, -1, SQLITE_TRANSIENT)
        }
    }
}

extension Data : StatementParameter {
    public init(from statement: Statement, index: Int) {
        assert(sqlite3_column_type(statement.handle, Int32(index)) == SQLITE_BLOB)
        if let pointer = sqlite3_column_blob(statement.handle, Int32(index)) {
            let length = Int(sqlite3_column_bytes(statement.handle, Int32(index)))
            self = Data(bytes: pointer, count: length)
        } else {
            self = Data()
        }
    }
    
    public func bind(from statement: Statement, index: Int) throws {
        try statement.connection.call {
            withUnsafeBytes { pointer in
                sqlite3_bind_blob(statement.handle, Int32(index), pointer.baseAddress, Int32(self.count), SQLITE_TRANSIENT)
            }
        }
    }
}

extension Date : StatementParameter {
    
    /// A `DateFormatter` for writing dates to the database
    private static var dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        return formatter
    }()
    
    public init(from statement: Statement, index: Int) {
        guard !statement.isNull(index: index),
              let pointer = sqlite3_column_text(statement.handle, Int32(index)) else {
            self = Date.distantPast
            return
        }

        let string = String(cString: pointer)
        guard let date = Date.dateFormatter.date(from: string) else {
            self = Date.distantPast
            return
        }

        self = date
    }
    
    public func bind(from statement: Statement, index: Int) throws {
        try statement.connection.call {
            let date = Date.dateFormatter.string(from: self)
            return sqlite3_bind_text(statement.handle, Int32(index), date.cString(using: .utf8), -1, SQLITE_TRANSIENT)
        }
    }
}
