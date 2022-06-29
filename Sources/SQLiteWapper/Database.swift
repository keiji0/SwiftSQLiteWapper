//
//  Database.swift
//  SQLiteWapper
//
//  Created by keiji0 on 2020/12/26.
//

import Foundation
import SQLite3

/// Connectionに依存しないsqlite apiを叩く場合にここから生やす
public final class Database {
    
    // MARK: - Static
    
    /// SQLIteのバージョンを取得
    public static var version: String? {
        guard let cString = sqlite3_libversion() else { return nil }
        return String(cString: cString)
    }
}
