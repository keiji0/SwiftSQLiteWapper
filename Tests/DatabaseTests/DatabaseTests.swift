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
    
    private func getTmpFile() -> URL {
        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent(UUID().uuidString)
    }

    static var allTests = [
        ("testExample", testOpen),
    ]
}
