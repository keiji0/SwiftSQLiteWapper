import XCTest
@testable import SQLiteWapper

final class DatabaseTests: XCTestCase {
    override func setUp() {
        super.setUp()
     }
    
    override func tearDown() {
        super.tearDown()
    }
    
    func testOpen() {
        let dbFile = getTmpFile()
        let db = Database(fileURL: dbFile)
        try! db.open()
        XCTAssertTrue(FileManager.default.fileExists(atPath: dbFile.path))
    }
    
    func test_テーブル一覧を取得できる() {
        let dbFile = getTmpFile()
        let db = Database(fileURL: dbFile)
        try! db.open()
        try! db.exec("CREATE TABLE TestTable ( date DATETIME );")
        XCTAssertEqual(db.tableNames, ["TestTable"])
    }
    
    private func getTmpFile() -> URL {
        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }
}
