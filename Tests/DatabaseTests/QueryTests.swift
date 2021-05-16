import XCTest
import Foundation
@testable import SQLiteWapper

final class QueryTests: XCTestCase {
    
    var db: Database!
    
    override func setUp() {
        super.setUp()
        let dbFile = getTmpFile()
        db = Database(fileURL: dbFile)
        try! db.open()
     }
    
    override func tearDown() {
        super.tearDown()
        try! db.close()
    }
    
    func testStatementDateParameter() {
        let now = Date()
        let formatter = ISO8601DateFormatter()
        try! db.exec("CREATE TABLE TestTable ( date DATETIME );")
        try! db.exec("INSERT INTO TestTable VALUES(?)", [ now ])
        try! db.query("SELECT date FROM TestTable;", [], {
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
            try! db.exec("CREATE TABLE \(table) ( val DOUBLE );")
            try! db.exec("INSERT INTO \(table) VALUES(?)", [ val ])
            try! db.query("SELECT val FROM \(table);", [], {
                try! $0.fetchRow({
                    print($0.column(Double.self, 0), val.description)
                    XCTAssertEqual($0.column(Double.self, 0), val)
                })
            })
            id += 1
        }
    }
    
    private func getTmpFile() -> URL {
        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }
}
