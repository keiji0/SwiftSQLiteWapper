//
//  ColumnType.swift
//  
//  
//  Created by keiji0 on 2022/07/01
//  
//

import SQLite3

/// カラムのプリミティブな型
/// https://www.sqlite.org/c3ref/c_blob.html
/// 基本型は全部で5つ
public enum ColumnType {
    case integer
    case float
    case blob
    case null
    case text
    
    /// APIの値からカラムインスタンスを生成
    init(_ sqlType: Int32) {
        switch sqlType {
        case SQLITE_INTEGER: self = .integer
        case SQLITE_FLOAT: self = .float
        case SQLITE_BLOB: self = .blob
        case SQLITE_NULL: self = .null
        case SQLITE_TEXT: self = .text
        default: fatalError()
        }
    }

    /// APIの値を取得
    var sqlType: Int32 {
        switch self {
        case .integer: return SQLITE_INTEGER
        case .float: return SQLITE_FLOAT
        case .blob: return SQLITE_BLOB
        case .null: return SQLITE_NULL
        case .text: return SQLITE_TEXT
        }
    }
}
