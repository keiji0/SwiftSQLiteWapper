import XCTest
import Foundation
@testable import SQLiteWapper

final class QueryTests: XCTestCase {
    
    func test_Intを保存できる() {
        let tableName = "TestTable"
        try! connection.exec("CREATE TABLE \(tableName) ( value INTEGER );")
        try! connection.exec("INSERT INTO \(tableName) VALUES(?)", [ 123 ])
        XCTAssertEqual(try! connection.fetchValue("SELECT value FROM \(tableName)"), 123)
    }
    
    func test_Int32を保存できる() {
        let tableName = "TestTable"
        try! connection.exec("CREATE TABLE \(tableName) ( value INTEGER );")
        try! connection.exec("INSERT INTO \(tableName) VALUES(?)", [ Int32(321) ])
        XCTAssertEqual(try! connection.fetchValue("SELECT value FROM \(tableName)"), Int32(321))
    }
    
    func test_Doubleを保存できる() {
        let tableName = "TestTable"
        try! connection.exec("CREATE TABLE \(tableName) ( value DOUBLE );")
        try! connection.exec("INSERT INTO \(tableName) VALUES(?)", [ 123.0 ])
        XCTAssertEqual(try! connection.fetchValue("SELECT value FROM \(tableName)"), 123.0)
    }
    
    func test_Stringを保存できる() {
        let tableName = "TestTable"
        try! connection.exec("CREATE TABLE \(tableName) ( value TEXT );")
        try! connection.exec("INSERT INTO \(tableName) VALUES(?)", [ "123" ])
        XCTAssertEqual(try! connection.fetchValue("SELECT value FROM \(tableName)"), "123")
    }
    
    func test_Dataを保存できる() {
        let tableName = "TestTable"
        try! connection.exec("CREATE TABLE \(tableName) ( value1, value2, value3 );")
        try! connection.exec("INSERT INTO \(tableName) VALUES(?, ?, ?)", [ 1, 2, 3 ])
        XCTAssertEqual(try! connection.query("SELECT value1 FROM \(tableName)").fetchRow()!.count, 1)
        XCTAssertEqual(try! connection.query("SELECT value1, value2 FROM \(tableName)").fetchRow()!.count, 2)
        XCTAssertEqual(try! connection.query("SELECT value1, value2, value3 FROM \(tableName)").fetchRow()!.count, 3)
    }
    
    func test_カラム数を取得() {
        let tableName = "TestTable"
        try! connection.exec("CREATE TABLE \(tableName) ( value BLOB );")
        try! connection.exec("INSERT INTO \(tableName) VALUES(?)", [ "123".data(using: .utf8)! ])
        XCTAssertEqual(try! connection.fetchValue("SELECT value FROM \(tableName)"), "123".data(using: .utf8)!)
    }
    
    func test_ロールバックできる() {
        let tableName = "TestTable"
        try! connection.exec("CREATE TABLE \(tableName) ( value )")
        try? connection.begin {
            try! connection.exec("INSERT INTO \(tableName) VALUES(?)", [ "123" ])
            throw DummyError()
        }
        XCTAssertEqual(try! connection.count("SELECT COUNT(*) FROM \(tableName)"), 0)
    }
    
    func test_行をシーケンスとして取得できる() {
        let tableName = "TestTable"
        let recoerds = ["1", "2", "3"]
        
        try! connection.exec("CREATE TABLE \(tableName) ( value TEXT );")
        recoerds.forEach {
            try! connection.exec("INSERT INTO \(tableName) VALUES(?)", [ $0 ])
        }
        
        do {
            let values: [String] = try! connection.query("SELECT value FROM \(tableName) ORDER BY value").fetchRows().map {
                $0.value(0)!
            }
            XCTAssertEqual(values, recoerds)
        }
        do {
            let values: [String] = try! connection.query("SELECT value FROM \(tableName) ORDER BY value DESC").fetchRows().map {
                $0.value(0)!
            }
            XCTAssertEqual(values, recoerds.reversed())
        }
    }
    
    func test_Cancelできる() {
        let dbFile = getTmpFile()
        
        // 適当なテーブルを作っておく
        do {
            let connection = try! Connection(dbFile)
            try! connection.exec("CREATE TABLE Hoge ( val );")
        }
        
        // Commit前にキャンセルする
        do {
            let connection = try! Connection(dbFile)
            
            let count = 500000
            let values = (0..<count).map{ _ in "(?)" }.joined(separator: ",")
            let params = (0..<count).map { _ in Int.random(in: 0...Int.max) }
            
            try! connection.exec("INSERT INTO Hoge VALUES \(values)", params)
            
            Task {
                try! await Task.sleep(nanoseconds: 5_000_000)
                connection.cancel()
            }

            XCTAssertThrowsError(try connection.count("SELECT COUNT(*) FROM Hoge WHERE val=?", [ "33" ])) {
                XCTAssertTrue(($0 as! DatabaseError).code == .interrupt)
            }
        }
    }
    
    func test_UserVersionはデフォルトは0() {
        XCTAssertEqual(connection.userVersion, 0)
    }
    
    func test_UserVersionが使用できる() {
        connection.userVersion = 8
        XCTAssertEqual(connection.userVersion, 8)
    }
    
    // MARK: -
    
    private lazy var connection: Connection = {
        try! .init(getTmpFile())
    }()
    
    private func getTmpFile() -> URL {
        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }
    
    private struct DummyError: Error {
    }
}

func funcTime(action: () -> Void) {
    let startDate = Date()
    action()
    let endDate = Date()
    print("\(endDate.timeIntervalSince(startDate))")
}
