//
//  Column.swift
//  
//  
//  Created by keiji0 on 2022/07/01
//  
//

import Foundation
import SQLite3

/// カラムの値
/// Bind時とSelectからの値を取得できる値を表すプロトコル
/// アプリケーションが必要であれば独自の型を追加することができる
public protocol Value {
    /// ステートメントに値をバインドする
    func bind(_ binder: Binder) throws
    /// 行から値を生成する
    init(_ value: Row.AnyValue) throws
}

// MARK: - Column Values

extension Int32 : Value {
    public func bind(_ binder: Binder) throws {
        try binder.bind(self)
    }
    
    public init(_ value: Row.AnyValue) throws {
        self = try value.int32
    }
}

extension Int64 : Value {
    public func bind(_ binder: Binder) throws {
        try binder.bind(self)
    }
    public init(_ value: Row.AnyValue) throws {
        self = try value.int64
    }
}

extension Int: Value {
    public func bind(_ binder: Binder) throws {
        try binder.bind(Int64(self))
    }
    public init(_ value: Row.AnyValue) throws {
        self = try Int(value.int64)
    }
}

extension Double: Value {
    public func bind(_ binder: Binder) throws {
        try binder.bind(self)
    }
    public init(_ value: Row.AnyValue) throws {
        self = try value.double
    }
}

extension String: Value {
    public func bind(_ binder: Binder) throws {
        try binder.bind(self)
    }
    public init(_ value: Row.AnyValue) throws {
        self = try value.string
    }
}

extension Data: Value {
    public func bind(_ binder: Binder) throws {
        try binder.bind(self)
    }
    public init(_ value: Row.AnyValue) throws {
        self = try value.data
    }
}

extension Bool: Value {
    public func bind(_ binder: Binder) throws {
        try binder.bind(Int32(self ? 1 : 0))
    }
    public init(_ value: Row.AnyValue) throws {
        self = try value.int32 != 0
    }
}
