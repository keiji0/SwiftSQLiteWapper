import XCTest
@testable import SQLiteWapper

final class DatabaseTests: XCTestCase {
    func testOpen() {
        let dbFile = getTmpFile()
        XCTAssertNoThrow(try Connection(dbFile))
        XCTAssertTrue(FileManager.default.fileExists(atPath: dbFile.path))
    }
    
    func test_DB接続失敗() {
        let dbFile = createNotWriteFile()
        XCTAssertThrowsError(try Connection(dbFile))
    }
    
    func test_テーブル一覧を取得できる() {
        let dbFile = getTmpFile()
        let db = try! Connection(dbFile)
        try! db.exec("CREATE TABLE TestTable ( date DATETIME );")
        XCTAssertEqual(db.tableNames, ["TestTable"])
    }
    
    func test_インメモリで開くことができる() {
        let db = try! Connection()
        try! db.exec("CREATE TABLE TestTable ( date DATETIME );")
        XCTAssertEqual(db.tableNames, ["TestTable"])
    }
    
    private func getTmpFile() -> URL {
        URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }
    
    private func createNotWriteFile() -> URL {
        URL(fileURLWithPath: "/").appendingPathComponent(UUID().uuidString)
    }
}
