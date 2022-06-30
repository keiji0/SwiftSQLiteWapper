//
//  Connection.swift
//  
//  
//  Created by keiji0 on 2022/06/29
//  
//

import Foundation
import SQLite3
import os

/// データベースとの接続
/// クエリはこのクラスから行う。参照が切れると自動的にデータベースはクローズされます
public final class Connection {
    
    /// 接続を開始
    /// - Parameters:
    ///   - fileURL: sqliteのファイルパス
    ///   - options: 接続オプション一覧
    public init(_ fileURL: URL, _ options: OpenOptions = .default) throws {
        self.fileURL = fileURL
        self.options = options
        do {
            Logger.main.info("Opening: version=\(Database.version ?? "<nil>"), path=\(self.fileURL.path)")
            try call { sqlite3_open_v2(fileURL.path, &handle, options.rawValue, nil) }
            Logger.main.info("Open success: path=\(self.fileURL.path)")
        } catch {
            throw error
        }
    }
    
    deinit {
        Logger.main.info("close \(self.fileURL.path)")
        statements.removeAll()
        do {
            try call {
                sqlite3_close(handle)
            }
        } catch let e {
            fatalError(e.localizedDescription)
        }
    }
    
    /// テーブルがロックされている時のビジータイムを設定
    /// - Parameter ms: ビジータイム（マイクロ秒）
    /// マルチスレッド接続の場合にこの設定がないとSQLITE_BUSYが発生する
    public func setBusyTimeout(_ ms: Int32) {
        try! call { sqlite3_busy_timeout(handle, ms) }
    }
    
    /// 変更された行の数を取得する
    public func changes() -> Int32 {
        sqlite3_changes(handle)
    }
    
    public func totalChanges() -> Int32 {
        sqlite3_total_changes(handle)
    }
    
    public func exec(_ sql: String, _ params: [StatementParameter] = []) throws {
        if params.isEmpty {
            try call {
                sqlite3_exec(handle, sql, nil, nil, nil)
            }
        } else {
            try prepare(sql).bind(params).step()
        }
    }
    
    public func prepare(_ sql: String) throws -> Statement {
        try Statement(self, sql: sql)
    }
    
    /// DBにクエリを投げる。クエリのステートメントはキャッシュされる
    @discardableResult
    public func query<T>(_ sql: String, _ params: [StatementParameter], _ block: (Statement) throws -> T) throws -> T {
        let statement = try prepareSql(sql)
        do {
            try statement.bind(params)
            let res = try block(statement)
            try statement.reset()
            return res
        } catch let e {
            try statement.reset()
            throw e
        }
    }
    
    public func query(_ sql: String, _ params: [StatementParameter]) throws {
        try query(sql, params, { statement in
            try statement.step()
        })
    }
    
    /// クエリ結果行数を取得
    public func count(_ sql: String, _ params: [StatementParameter] = []) throws -> Int {
        try query(sql, params) { statment in
            try statment.fetchRow { row in
                row.column(Int.self, 0)
            } ?? 0
        }
    }
    
    public func begin() throws {
        defer { transactionNestLevel += 1 }
        guard transactionNestLevel == 0 else { return }
        try exec("BEGIN;")
    }
    
    public func end() throws {
        defer { transactionNestLevel -= 1 }
        guard transactionNestLevel == 1 else { return }
        try exec("COMMIT;")
    }
    
    /// 定義されている全てのテーブル名を取得
    public var tableNames: [String] {
        get throws {
            try prepare("SELECT tbl_name FROM sqlite_master WHERE type='table'").fetchRows {
                $0.column(String.self, 0)
            }
        }
    }
    
    /// キャンセル
    /// どのスレッドから実行しても問題ない
    public func cancel() {
        Logger.main.info("cancel: path=\(self.fileURL.path)")
        sqlite3_interrupt(handle)
    }
    
    // MARK: - Internal
    
    var handle: OpaquePointer?
    
    /// sqliteのAPIをコールする
    /// エラーが発生すると例外を投げる
    @discardableResult
    func call(block: () -> (Int32)) throws -> QueryResult {
        let result = QueryResult.code(for: block())
        switch result {
        case .ok, .done, .row:
            return result
        case .error(let code):
            throw DatabaseError(code: code, message: String(cString: sqlite3_errmsg(handle)))
        }
    }
    
    // MARK: - Private
    
    private let fileURL: URL
    private let options: OpenOptions
    private var transactionNestLevel: Int = 0
    private var statements = [String: Statement]()
    
    /// 同一SQL文のステートメントはキャッシュする。これによって10-20%ほど早くなった
    private func prepareSql(_ sql: String) throws -> Statement {
        if let statement = statements[sql] {
            return statement
        } else {
            let statement = try prepare(sql)
            statements[sql] = statement
            return statement
        }
    }
}
