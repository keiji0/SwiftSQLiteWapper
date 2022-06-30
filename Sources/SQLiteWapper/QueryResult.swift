//
//  QueryResult.swift
//  
//
//  Created by keiji0 on 2020/12/27.
//

import Foundation
import SQLite3

/// クエリ結果
public enum QueryResult : Equatable {
    case ok
    case done
    case row
    case error(Int32)
    
    static func code(for code: Int32) -> QueryResult {
        switch code {
        case SQLITE_OK:
            return .ok
        case SQLITE_DONE:
            return .done
        case SQLITE_ROW:
            return .row
        // 上記３つ以外はエラーとみなす
        default:
            return .error(code)
        }
    }
}
