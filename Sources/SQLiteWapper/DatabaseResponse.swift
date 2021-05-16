//
//  File.swift
//  
//
//  Created by keiji0 on 2020/12/27.
//

import Foundation
import SQLite3

/// データベースからのレスポンス情報
public enum DatabaseResponse : Equatable {
    
    case ok
    case done
    case row
    case error(Int32)
    
    static func code(for code: Int32) -> DatabaseResponse {
        switch code {
        case SQLITE_OK:  return .ok
        case SQLITE_DONE:return .done
        case SQLITE_ROW: return .row
        default:         return .error(code)
        }
    }
}
