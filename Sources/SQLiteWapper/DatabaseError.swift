//
//  DatabaseError.swift
//  
//
//  Created by keiji0 on 2020/12/27.
//

import Foundation

/// データベースからのエラー
public enum DatabaseError: Error {
    
    case api(Int32, String)
    case notOpened
    case noStatementPrepared
    case closeFailed
    case notFound
}

