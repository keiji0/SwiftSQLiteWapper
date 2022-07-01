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
            
            
            try! connection.begin()
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
            
            try! connection.end()
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
    
    private func getTmpFile() -> URL {
        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }
}

func funcTime(action: () -> Void) {
    let startDate = Date()
    action()
    let endDate = Date()
    print("\(endDate.timeIntervalSince(startDate))")
}
