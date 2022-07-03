//
//  UserPreferenceTests.swift
//  
//  
//  Created by keiji0 on 2022/07/01
//  
//

import Foundation
import XCTest
@testable import SQLiteWapper

final class UserPreferenceTests: XCTestCase {
    func test_テーブルが生成されている() {
        XCTAssertTrue(connection.hasTable(userPreference.tableName))
    }
    
    func test_何も設定されていないとnilになる() {
        XCTAssertNil(try userPreference.get("foo", Int.self))
    }
    
    func test_適当な値を設定() {
        try! userPreference.set("a", "123")
        XCTAssertEqual(try userPreference.get("a"), "123")
    }
    
    func test_型が違うとnilになる() {
        try! userPreference.set("a", "123")
        XCTAssertNil(try! userPreference.get("a", Int.self))
    }
    
    func test_Key一覧を取得できる() {
        try! userPreference.set("a", "123")
        try! userPreference.set("b", "321")
        XCTAssertEqual(userPreference.keys, ["a", "b"])
    }
    
    func test_全てのデータを消去できる() {
        try! userPreference.set("a", "123")
        try! userPreference.set("b", "321")
        try! userPreference.removeAll()
        XCTAssertEqual(userPreference.keys, [])
    }
    
    // MARK: -
    
    private lazy var userPreference: UserPreference = {
        try! .init(self.connection)
    }()
    
    private lazy var connection: Connection = {
        try! .init(getTmpFile())
    }()
    
    private func getTmpFile() -> URL {
        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }
}
