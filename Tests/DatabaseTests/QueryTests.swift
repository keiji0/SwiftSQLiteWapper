import XCTest
import Foundation
@testable import SQLiteWapper

final class QueryTests: XCTestCase {
    
    var connection: Connection!
    
    override func setUpWithError() throws {
        super.setUp()
        let dbFile = getTmpFile()
        connection = try Connection(dbFile)
     }
    
    override func tearDown() {
        super.tearDown()
        connection = nil
    }
    
    func testStatementDateParameter() {
        let now = Date()
        let formatter = ISO8601DateFormatter()
        try! connection.exec("CREATE TABLE TestTable ( date DATETIME );")
        try! connection.exec("INSERT INTO TestTable VALUES(?)", [ now ])
        try! connection.query("SELECT date FROM TestTable;", [], {
            try! $0.fetchRow({
                let a = formatter.string(from: now)
                let b = formatter.string(from: $0.column(Date.self, 0))
                XCTAssertEqual(a, b)
            })
        })
    }
    
    func testStatementDoubleParameter() {
        let now = Date()
        let vals = [
            Date().timeIntervalSince(now),
            Date().timeIntervalSince(now),
            Date().timeIntervalSince(now),
            Date().timeIntervalSince(now),
        ]
        var id = 0
        for val in vals {
            let table = "Hoge\(id)"
            try! connection.exec("CREATE TABLE \(table) ( val DOUBLE );")
            try! connection.exec("INSERT INTO \(table) VALUES(?)", [ val ])
            try! connection.query("SELECT val FROM \(table);", [], {
                try! $0.fetchRow({
                    print($0.column(Double.self, 0), val.description)
                    XCTAssertEqual($0.column(Double.self, 0), val)
                })
            })
            id += 1
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
            
            Task {
                try! await Task.sleep(nanoseconds: 500_000_000)
                connection.cancel()
            }
            
            connection.begin()
            try! connection.exec("INSERT INTO Hoge VALUES(?)", [ "abc" ])
            sleep(1)
        }
        
        // キャンセルが成功しているはずなのでInsertは無効になっているはず
        do {
            let connection = try! Connection(dbFile)
            XCTAssertEqual(try! connection.count("SELECT COUNT(*) FROM Hoge"), 0)
        }
    }
    
    // MARK: -
    
    private func getTmpFile() -> URL {
        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }
}
