//
//  UserPreference.swift
//  
//  
//  Created by keiji0 on 2022/07/01
//  
//

import Foundation
import os

/// SQLite上に作る簡易KVStore
///
/// * Codableに適用した値であれば格納できる
/// * 値はJSONEncodeされるのでリレーションして使う感じではなく単なるKVStore
/// * 他のテーブルとアトミックに更新を行いたい場合はConnection.beginで制御する
public final class UserPreference {
    /// 初期化処理
    /// - Parameters:
    ///   - connection: コネクション
    ///   - tableName: テーブル名
    /// 初期化時にテーブルが存在しなければ自動的に生成する
    public init(_ connection: Connection, tableName: String = "UserPreference") throws {
        self.connection = connection
        self.tableName = tableName
        try setup()
    }
    
    /// 全てのキーを取得
    public var keys: [String] {
        try! connection.fetchValues("SELECT key FROM \(tableName)")
    }
    
    /// キーを指定して値を取得
    /// 存在しないキーの場合はnilが返る。またデコードできなかった場合もnilが返る
    public func get<Value: Decodable>(_ key: String) throws -> Value? {
        guard let text: String = try connection.fetchValue("SELECT value FROM \(tableName) WHERE key = ?", [key]) else {
            return nil
        }
        return try? decode(text)
    }
    
    /// キーを指定して値を取得(型指定)
    public func get<Value: Decodable>(_ key: String, _ type: Value.Type) throws -> Value? {
        let res: Value? = try get(key)
        return res
    }
    
    /// 指定したキーに値を設定
    public func set(_ key: String, _ value: some Encodable) throws {
        let encodingValue = try encode(value)
        try connection.exec(
            "INSERT INTO \(tableName)(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET key=?, value=?",
            [
                key, encodingValue,
                key, encodingValue,
            ]
        )
    }
    
    /// 全てのデータを消去する
    public func removeAll() throws {
        try connection.exec("DELETE FROM \(tableName)")
    }
    
    // MARK: - Private
    
    private let connection: Connection
    public let tableName: String
    
    private func setup() throws {
        try loadSchema()
    }
    
    private func loadSchema() throws {
        if connection.hasTable(tableName) {
            return
        }
        try connection.exec(schema(tableName))
    }
    
    private func encode(_ value: some Encodable) throws -> String {
        let encoder = JSONEncoder()
        return String(data: try encoder.encode(value), encoding: .utf8)!
    }
    
    private func decode<Value: Decodable>(_ value: String) throws -> Value {
        let decoder = JSONDecoder()
        return try decoder.decode(Value.self, from: value.data(using: .utf8)!)
    }
}

private func schema(_ tableName: String) -> String {
"""
CREATE TABLE \(tableName) (
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (key)
)
"""
}
