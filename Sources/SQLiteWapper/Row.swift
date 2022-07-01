//
//  Row.swift
//  
//  
//  Created by keiji0 on 2022/07/01
//  
//

import Foundation
import SQLite3

/// 行を表すモデル
public struct Row {
    /// 指定カラムがnilかどうか？
    public func isNull(_ index: Int) -> Bool {
        type(index) == .null
    }
    
    /// カラムのタイプを取得
    public func type(_ index: Int) -> ColumnType {
        .init(sqlite3_column_type(handle, Int32(index)))
    }
    
    /// カラム数を取得
    public var count: Int {
        Int(sqlite3_column_count(handle))
    }

    /// 指定カラムの値を取得
    public func value<T: Value>(_ index: Int) -> T? {
        try? .init(.init(handle, index))
    }
    
    /// 指定からの値を取得（Type指定）
    public func value<T: Value>(_ index: Int, _ type: T.Type) -> T? {
        try? .init(.init(handle, index))
    }
    
    public struct ValueError : Error {
        var localizedDescription: String { "Wrong type." }
    }
    
    /// 値へのラッパークラス
    public struct AnyValue {
        fileprivate init(_ handle: OpaquePointer, _ index: Int) {
            self.handle = handle
            self.index = index
        }
        private let handle: OpaquePointer
        private let index: Int
        
        public var int32: Int32 {
            get throws {
                try checkValue(.integer)
                return sqlite3_column_int(handle, Int32(index))
            }
        }
        
        public var int64: Int64 {
            get throws {
                try checkValue(.integer)
                return sqlite3_column_int64(handle, Int32(index))
            }
        }
        
        public var double: Double {
            get throws {
                try checkValue(.float)
                return sqlite3_column_double(handle, Int32(index))
            }
        }
        
        public var string: String {
            get throws {
                try checkValue(.text)
                return String(cString: UnsafePointer(sqlite3_column_text(handle, Int32(index))))
            }
        }
        
        public var data: Data {
            get throws {
                try checkValue(.blob)
                guard let pointer = sqlite3_column_blob(handle, Int32(index)) else {
                    return .init()
                }
                let length = Int(sqlite3_column_bytes(handle, Int32(index)))
                return Data(bytes: pointer, count: length)
            }
        }
        
        public var isNull: Bool {
            type == .null
        }
        
        public var type: ColumnType {
            .init(sqlite3_column_type(handle, Int32(index)))
        }
        
        private func checkValue(_ type: ColumnType) throws {
            guard self.type == type else {
                throw ValueError()
            }
        }
    }

    // MARK: - Internal

    init(_ handle: OpaquePointer) {
        self.handle = handle
    }

    // MARK: - Private

    let handle: OpaquePointer
}
