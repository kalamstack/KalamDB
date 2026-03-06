// GENERATED CODE - DO NOT MODIFY BY HAND
// coverage:ignore-file
// ignore_for_file: type=lint
// ignore_for_file: unused_element, deprecated_member_use, deprecated_member_use_from_same_package, use_function_type_syntax_for_parameters, unnecessary_const, avoid_init_to_null, invalid_override_different_default_values_named, prefer_expression_function_bodies, annotate_overrides, invalid_annotation_target, unnecessary_question_mark

part of 'models.dart';

// **************************************************************************
// FreezedGenerator
// **************************************************************************

// dart format off
T _$identity<T>(T value) => value;

/// @nodoc
mixin _$DartAuthProvider {
  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType && other is DartAuthProvider);
  }

  @override
  int get hashCode => runtimeType.hashCode;

  @override
  String toString() {
    return 'DartAuthProvider()';
  }
}

/// @nodoc
class $DartAuthProviderCopyWith<$Res> {
  $DartAuthProviderCopyWith(
      DartAuthProvider _, $Res Function(DartAuthProvider) __);
}

/// Adds pattern-matching-related methods to [DartAuthProvider].
extension DartAuthProviderPatterns on DartAuthProvider {
  /// A variant of `map` that fallback to returning `orElse`.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case _:
  ///     return orElse();
  /// }
  /// ```

  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(DartAuthProvider_BasicAuth value)? basicAuth,
    TResult Function(DartAuthProvider_JwtToken value)? jwtToken,
    TResult Function(DartAuthProvider_None value)? none,
    required TResult orElse(),
  }) {
    final _that = this;
    switch (_that) {
      case DartAuthProvider_BasicAuth() when basicAuth != null:
        return basicAuth(_that);
      case DartAuthProvider_JwtToken() when jwtToken != null:
        return jwtToken(_that);
      case DartAuthProvider_None() when none != null:
        return none(_that);
      case _:
        return orElse();
    }
  }

  /// A `switch`-like method, using callbacks.
  ///
  /// Callbacks receives the raw object, upcasted.
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case final Subclass2 value:
  ///     return ...;
  /// }
  /// ```

  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(DartAuthProvider_BasicAuth value) basicAuth,
    required TResult Function(DartAuthProvider_JwtToken value) jwtToken,
    required TResult Function(DartAuthProvider_None value) none,
  }) {
    final _that = this;
    switch (_that) {
      case DartAuthProvider_BasicAuth():
        return basicAuth(_that);
      case DartAuthProvider_JwtToken():
        return jwtToken(_that);
      case DartAuthProvider_None():
        return none(_that);
    }
  }

  /// A variant of `map` that fallback to returning `null`.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case _:
  ///     return null;
  /// }
  /// ```

  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(DartAuthProvider_BasicAuth value)? basicAuth,
    TResult? Function(DartAuthProvider_JwtToken value)? jwtToken,
    TResult? Function(DartAuthProvider_None value)? none,
  }) {
    final _that = this;
    switch (_that) {
      case DartAuthProvider_BasicAuth() when basicAuth != null:
        return basicAuth(_that);
      case DartAuthProvider_JwtToken() when jwtToken != null:
        return jwtToken(_that);
      case DartAuthProvider_None() when none != null:
        return none(_that);
      case _:
        return null;
    }
  }

  /// A variant of `when` that fallback to an `orElse` callback.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case _:
  ///     return orElse();
  /// }
  /// ```

  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String username, String password)? basicAuth,
    TResult Function(String token)? jwtToken,
    TResult Function()? none,
    required TResult orElse(),
  }) {
    final _that = this;
    switch (_that) {
      case DartAuthProvider_BasicAuth() when basicAuth != null:
        return basicAuth(_that.username, _that.password);
      case DartAuthProvider_JwtToken() when jwtToken != null:
        return jwtToken(_that.token);
      case DartAuthProvider_None() when none != null:
        return none();
      case _:
        return orElse();
    }
  }

  /// A `switch`-like method, using callbacks.
  ///
  /// As opposed to `map`, this offers destructuring.
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case Subclass2(:final field2):
  ///     return ...;
  /// }
  /// ```

  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String username, String password) basicAuth,
    required TResult Function(String token) jwtToken,
    required TResult Function() none,
  }) {
    final _that = this;
    switch (_that) {
      case DartAuthProvider_BasicAuth():
        return basicAuth(_that.username, _that.password);
      case DartAuthProvider_JwtToken():
        return jwtToken(_that.token);
      case DartAuthProvider_None():
        return none();
    }
  }

  /// A variant of `when` that fallback to returning `null`
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case _:
  ///     return null;
  /// }
  /// ```

  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String username, String password)? basicAuth,
    TResult? Function(String token)? jwtToken,
    TResult? Function()? none,
  }) {
    final _that = this;
    switch (_that) {
      case DartAuthProvider_BasicAuth() when basicAuth != null:
        return basicAuth(_that.username, _that.password);
      case DartAuthProvider_JwtToken() when jwtToken != null:
        return jwtToken(_that.token);
      case DartAuthProvider_None() when none != null:
        return none();
      case _:
        return null;
    }
  }
}

/// @nodoc

class DartAuthProvider_BasicAuth extends DartAuthProvider {
  const DartAuthProvider_BasicAuth(
      {required this.username, required this.password})
      : super._();

  final String username;
  final String password;

  /// Create a copy of DartAuthProvider
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartAuthProvider_BasicAuthCopyWith<DartAuthProvider_BasicAuth>
      get copyWith =>
          _$DartAuthProvider_BasicAuthCopyWithImpl<DartAuthProvider_BasicAuth>(
              this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartAuthProvider_BasicAuth &&
            (identical(other.username, username) ||
                other.username == username) &&
            (identical(other.password, password) ||
                other.password == password));
  }

  @override
  int get hashCode => Object.hash(runtimeType, username, password);

  @override
  String toString() {
    return 'DartAuthProvider.basicAuth(username: $username, password: $password)';
  }
}

/// @nodoc
abstract mixin class $DartAuthProvider_BasicAuthCopyWith<$Res>
    implements $DartAuthProviderCopyWith<$Res> {
  factory $DartAuthProvider_BasicAuthCopyWith(DartAuthProvider_BasicAuth value,
          $Res Function(DartAuthProvider_BasicAuth) _then) =
      _$DartAuthProvider_BasicAuthCopyWithImpl;
  @useResult
  $Res call({String username, String password});
}

/// @nodoc
class _$DartAuthProvider_BasicAuthCopyWithImpl<$Res>
    implements $DartAuthProvider_BasicAuthCopyWith<$Res> {
  _$DartAuthProvider_BasicAuthCopyWithImpl(this._self, this._then);

  final DartAuthProvider_BasicAuth _self;
  final $Res Function(DartAuthProvider_BasicAuth) _then;

  /// Create a copy of DartAuthProvider
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  $Res call({
    Object? username = null,
    Object? password = null,
  }) {
    return _then(DartAuthProvider_BasicAuth(
      username: null == username
          ? _self.username
          : username // ignore: cast_nullable_to_non_nullable
              as String,
      password: null == password
          ? _self.password
          : password // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class DartAuthProvider_JwtToken extends DartAuthProvider {
  const DartAuthProvider_JwtToken({required this.token}) : super._();

  final String token;

  /// Create a copy of DartAuthProvider
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartAuthProvider_JwtTokenCopyWith<DartAuthProvider_JwtToken> get copyWith =>
      _$DartAuthProvider_JwtTokenCopyWithImpl<DartAuthProvider_JwtToken>(
          this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartAuthProvider_JwtToken &&
            (identical(other.token, token) || other.token == token));
  }

  @override
  int get hashCode => Object.hash(runtimeType, token);

  @override
  String toString() {
    return 'DartAuthProvider.jwtToken(token: $token)';
  }
}

/// @nodoc
abstract mixin class $DartAuthProvider_JwtTokenCopyWith<$Res>
    implements $DartAuthProviderCopyWith<$Res> {
  factory $DartAuthProvider_JwtTokenCopyWith(DartAuthProvider_JwtToken value,
          $Res Function(DartAuthProvider_JwtToken) _then) =
      _$DartAuthProvider_JwtTokenCopyWithImpl;
  @useResult
  $Res call({String token});
}

/// @nodoc
class _$DartAuthProvider_JwtTokenCopyWithImpl<$Res>
    implements $DartAuthProvider_JwtTokenCopyWith<$Res> {
  _$DartAuthProvider_JwtTokenCopyWithImpl(this._self, this._then);

  final DartAuthProvider_JwtToken _self;
  final $Res Function(DartAuthProvider_JwtToken) _then;

  /// Create a copy of DartAuthProvider
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  $Res call({
    Object? token = null,
  }) {
    return _then(DartAuthProvider_JwtToken(
      token: null == token
          ? _self.token
          : token // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class DartAuthProvider_None extends DartAuthProvider {
  const DartAuthProvider_None() : super._();

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType && other is DartAuthProvider_None);
  }

  @override
  int get hashCode => runtimeType.hashCode;

  @override
  String toString() {
    return 'DartAuthProvider.none()';
  }
}

/// @nodoc
mixin _$DartChangeEvent {
  String get subscriptionId;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartChangeEventCopyWith<DartChangeEvent> get copyWith =>
      _$DartChangeEventCopyWithImpl<DartChangeEvent>(
          this as DartChangeEvent, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartChangeEvent &&
            (identical(other.subscriptionId, subscriptionId) ||
                other.subscriptionId == subscriptionId));
  }

  @override
  int get hashCode => Object.hash(runtimeType, subscriptionId);

  @override
  String toString() {
    return 'DartChangeEvent(subscriptionId: $subscriptionId)';
  }
}

/// @nodoc
abstract mixin class $DartChangeEventCopyWith<$Res> {
  factory $DartChangeEventCopyWith(
          DartChangeEvent value, $Res Function(DartChangeEvent) _then) =
      _$DartChangeEventCopyWithImpl;
  @useResult
  $Res call({String subscriptionId});
}

/// @nodoc
class _$DartChangeEventCopyWithImpl<$Res>
    implements $DartChangeEventCopyWith<$Res> {
  _$DartChangeEventCopyWithImpl(this._self, this._then);

  final DartChangeEvent _self;
  final $Res Function(DartChangeEvent) _then;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? subscriptionId = null,
  }) {
    return _then(_self.copyWith(
      subscriptionId: null == subscriptionId
          ? _self.subscriptionId
          : subscriptionId // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// Adds pattern-matching-related methods to [DartChangeEvent].
extension DartChangeEventPatterns on DartChangeEvent {
  /// A variant of `map` that fallback to returning `orElse`.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case _:
  ///     return orElse();
  /// }
  /// ```

  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(DartChangeEvent_Ack value)? ack,
    TResult Function(DartChangeEvent_InitialDataBatch value)? initialDataBatch,
    TResult Function(DartChangeEvent_Insert value)? insert,
    TResult Function(DartChangeEvent_Update value)? update,
    TResult Function(DartChangeEvent_Delete value)? delete,
    TResult Function(DartChangeEvent_Error value)? error,
    required TResult orElse(),
  }) {
    final _that = this;
    switch (_that) {
      case DartChangeEvent_Ack() when ack != null:
        return ack(_that);
      case DartChangeEvent_InitialDataBatch() when initialDataBatch != null:
        return initialDataBatch(_that);
      case DartChangeEvent_Insert() when insert != null:
        return insert(_that);
      case DartChangeEvent_Update() when update != null:
        return update(_that);
      case DartChangeEvent_Delete() when delete != null:
        return delete(_that);
      case DartChangeEvent_Error() when error != null:
        return error(_that);
      case _:
        return orElse();
    }
  }

  /// A `switch`-like method, using callbacks.
  ///
  /// Callbacks receives the raw object, upcasted.
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case final Subclass2 value:
  ///     return ...;
  /// }
  /// ```

  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(DartChangeEvent_Ack value) ack,
    required TResult Function(DartChangeEvent_InitialDataBatch value)
        initialDataBatch,
    required TResult Function(DartChangeEvent_Insert value) insert,
    required TResult Function(DartChangeEvent_Update value) update,
    required TResult Function(DartChangeEvent_Delete value) delete,
    required TResult Function(DartChangeEvent_Error value) error,
  }) {
    final _that = this;
    switch (_that) {
      case DartChangeEvent_Ack():
        return ack(_that);
      case DartChangeEvent_InitialDataBatch():
        return initialDataBatch(_that);
      case DartChangeEvent_Insert():
        return insert(_that);
      case DartChangeEvent_Update():
        return update(_that);
      case DartChangeEvent_Delete():
        return delete(_that);
      case DartChangeEvent_Error():
        return error(_that);
    }
  }

  /// A variant of `map` that fallback to returning `null`.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case _:
  ///     return null;
  /// }
  /// ```

  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(DartChangeEvent_Ack value)? ack,
    TResult? Function(DartChangeEvent_InitialDataBatch value)? initialDataBatch,
    TResult? Function(DartChangeEvent_Insert value)? insert,
    TResult? Function(DartChangeEvent_Update value)? update,
    TResult? Function(DartChangeEvent_Delete value)? delete,
    TResult? Function(DartChangeEvent_Error value)? error,
  }) {
    final _that = this;
    switch (_that) {
      case DartChangeEvent_Ack() when ack != null:
        return ack(_that);
      case DartChangeEvent_InitialDataBatch() when initialDataBatch != null:
        return initialDataBatch(_that);
      case DartChangeEvent_Insert() when insert != null:
        return insert(_that);
      case DartChangeEvent_Update() when update != null:
        return update(_that);
      case DartChangeEvent_Delete() when delete != null:
        return delete(_that);
      case DartChangeEvent_Error() when error != null:
        return error(_that);
      case _:
        return null;
    }
  }

  /// A variant of `when` that fallback to an `orElse` callback.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case _:
  ///     return orElse();
  /// }
  /// ```

  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(
            String subscriptionId,
            int totalRows,
            List<DartSchemaField> schema,
            int batchNum,
            bool hasMore,
            String status)?
        ack,
    TResult Function(String subscriptionId, List<String> rowsJson, int batchNum,
            bool hasMore, String status)?
        initialDataBatch,
    TResult Function(String subscriptionId, List<String> rowsJson)? insert,
    TResult Function(String subscriptionId, List<String> rowsJson,
            List<String> oldRowsJson)?
        update,
    TResult Function(String subscriptionId, List<String> oldRowsJson)? delete,
    TResult Function(String subscriptionId, String code, String message)? error,
    required TResult orElse(),
  }) {
    final _that = this;
    switch (_that) {
      case DartChangeEvent_Ack() when ack != null:
        return ack(_that.subscriptionId, _that.totalRows, _that.schema,
            _that.batchNum, _that.hasMore, _that.status);
      case DartChangeEvent_InitialDataBatch() when initialDataBatch != null:
        return initialDataBatch(_that.subscriptionId, _that.rowsJson,
            _that.batchNum, _that.hasMore, _that.status);
      case DartChangeEvent_Insert() when insert != null:
        return insert(_that.subscriptionId, _that.rowsJson);
      case DartChangeEvent_Update() when update != null:
        return update(_that.subscriptionId, _that.rowsJson, _that.oldRowsJson);
      case DartChangeEvent_Delete() when delete != null:
        return delete(_that.subscriptionId, _that.oldRowsJson);
      case DartChangeEvent_Error() when error != null:
        return error(_that.subscriptionId, _that.code, _that.message);
      case _:
        return orElse();
    }
  }

  /// A `switch`-like method, using callbacks.
  ///
  /// As opposed to `map`, this offers destructuring.
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case Subclass2(:final field2):
  ///     return ...;
  /// }
  /// ```

  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(
            String subscriptionId,
            int totalRows,
            List<DartSchemaField> schema,
            int batchNum,
            bool hasMore,
            String status)
        ack,
    required TResult Function(String subscriptionId, List<String> rowsJson,
            int batchNum, bool hasMore, String status)
        initialDataBatch,
    required TResult Function(String subscriptionId, List<String> rowsJson)
        insert,
    required TResult Function(String subscriptionId, List<String> rowsJson,
            List<String> oldRowsJson)
        update,
    required TResult Function(String subscriptionId, List<String> oldRowsJson)
        delete,
    required TResult Function(
            String subscriptionId, String code, String message)
        error,
  }) {
    final _that = this;
    switch (_that) {
      case DartChangeEvent_Ack():
        return ack(_that.subscriptionId, _that.totalRows, _that.schema,
            _that.batchNum, _that.hasMore, _that.status);
      case DartChangeEvent_InitialDataBatch():
        return initialDataBatch(_that.subscriptionId, _that.rowsJson,
            _that.batchNum, _that.hasMore, _that.status);
      case DartChangeEvent_Insert():
        return insert(_that.subscriptionId, _that.rowsJson);
      case DartChangeEvent_Update():
        return update(_that.subscriptionId, _that.rowsJson, _that.oldRowsJson);
      case DartChangeEvent_Delete():
        return delete(_that.subscriptionId, _that.oldRowsJson);
      case DartChangeEvent_Error():
        return error(_that.subscriptionId, _that.code, _that.message);
    }
  }

  /// A variant of `when` that fallback to returning `null`
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case _:
  ///     return null;
  /// }
  /// ```

  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(
            String subscriptionId,
            int totalRows,
            List<DartSchemaField> schema,
            int batchNum,
            bool hasMore,
            String status)?
        ack,
    TResult? Function(String subscriptionId, List<String> rowsJson,
            int batchNum, bool hasMore, String status)?
        initialDataBatch,
    TResult? Function(String subscriptionId, List<String> rowsJson)? insert,
    TResult? Function(String subscriptionId, List<String> rowsJson,
            List<String> oldRowsJson)?
        update,
    TResult? Function(String subscriptionId, List<String> oldRowsJson)? delete,
    TResult? Function(String subscriptionId, String code, String message)?
        error,
  }) {
    final _that = this;
    switch (_that) {
      case DartChangeEvent_Ack() when ack != null:
        return ack(_that.subscriptionId, _that.totalRows, _that.schema,
            _that.batchNum, _that.hasMore, _that.status);
      case DartChangeEvent_InitialDataBatch() when initialDataBatch != null:
        return initialDataBatch(_that.subscriptionId, _that.rowsJson,
            _that.batchNum, _that.hasMore, _that.status);
      case DartChangeEvent_Insert() when insert != null:
        return insert(_that.subscriptionId, _that.rowsJson);
      case DartChangeEvent_Update() when update != null:
        return update(_that.subscriptionId, _that.rowsJson, _that.oldRowsJson);
      case DartChangeEvent_Delete() when delete != null:
        return delete(_that.subscriptionId, _that.oldRowsJson);
      case DartChangeEvent_Error() when error != null:
        return error(_that.subscriptionId, _that.code, _that.message);
      case _:
        return null;
    }
  }
}

/// @nodoc

class DartChangeEvent_Ack extends DartChangeEvent {
  const DartChangeEvent_Ack(
      {required this.subscriptionId,
      required this.totalRows,
      required final List<DartSchemaField> schema,
      required this.batchNum,
      required this.hasMore,
      required this.status})
      : _schema = schema,
        super._();

  @override
  final String subscriptionId;
  final int totalRows;
  final List<DartSchemaField> _schema;
  List<DartSchemaField> get schema {
    if (_schema is EqualUnmodifiableListView) return _schema;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_schema);
  }

  final int batchNum;
  final bool hasMore;
  final String status;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartChangeEvent_AckCopyWith<DartChangeEvent_Ack> get copyWith =>
      _$DartChangeEvent_AckCopyWithImpl<DartChangeEvent_Ack>(this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartChangeEvent_Ack &&
            (identical(other.subscriptionId, subscriptionId) ||
                other.subscriptionId == subscriptionId) &&
            (identical(other.totalRows, totalRows) ||
                other.totalRows == totalRows) &&
            const DeepCollectionEquality().equals(other._schema, _schema) &&
            (identical(other.batchNum, batchNum) ||
                other.batchNum == batchNum) &&
            (identical(other.hasMore, hasMore) || other.hasMore == hasMore) &&
            (identical(other.status, status) || other.status == status));
  }

  @override
  int get hashCode => Object.hash(runtimeType, subscriptionId, totalRows,
      const DeepCollectionEquality().hash(_schema), batchNum, hasMore, status);

  @override
  String toString() {
    return 'DartChangeEvent.ack(subscriptionId: $subscriptionId, totalRows: $totalRows, schema: $schema, batchNum: $batchNum, hasMore: $hasMore, status: $status)';
  }
}

/// @nodoc
abstract mixin class $DartChangeEvent_AckCopyWith<$Res>
    implements $DartChangeEventCopyWith<$Res> {
  factory $DartChangeEvent_AckCopyWith(
          DartChangeEvent_Ack value, $Res Function(DartChangeEvent_Ack) _then) =
      _$DartChangeEvent_AckCopyWithImpl;
  @override
  @useResult
  $Res call(
      {String subscriptionId,
      int totalRows,
      List<DartSchemaField> schema,
      int batchNum,
      bool hasMore,
      String status});
}

/// @nodoc
class _$DartChangeEvent_AckCopyWithImpl<$Res>
    implements $DartChangeEvent_AckCopyWith<$Res> {
  _$DartChangeEvent_AckCopyWithImpl(this._self, this._then);

  final DartChangeEvent_Ack _self;
  final $Res Function(DartChangeEvent_Ack) _then;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @pragma('vm:prefer-inline')
  $Res call({
    Object? subscriptionId = null,
    Object? totalRows = null,
    Object? schema = null,
    Object? batchNum = null,
    Object? hasMore = null,
    Object? status = null,
  }) {
    return _then(DartChangeEvent_Ack(
      subscriptionId: null == subscriptionId
          ? _self.subscriptionId
          : subscriptionId // ignore: cast_nullable_to_non_nullable
              as String,
      totalRows: null == totalRows
          ? _self.totalRows
          : totalRows // ignore: cast_nullable_to_non_nullable
              as int,
      schema: null == schema
          ? _self._schema
          : schema // ignore: cast_nullable_to_non_nullable
              as List<DartSchemaField>,
      batchNum: null == batchNum
          ? _self.batchNum
          : batchNum // ignore: cast_nullable_to_non_nullable
              as int,
      hasMore: null == hasMore
          ? _self.hasMore
          : hasMore // ignore: cast_nullable_to_non_nullable
              as bool,
      status: null == status
          ? _self.status
          : status // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class DartChangeEvent_InitialDataBatch extends DartChangeEvent {
  const DartChangeEvent_InitialDataBatch(
      {required this.subscriptionId,
      required final List<String> rowsJson,
      required this.batchNum,
      required this.hasMore,
      required this.status})
      : _rowsJson = rowsJson,
        super._();

  @override
  final String subscriptionId;

  /// Each entry is a JSON-encoded row object (`{"col": value, ...}`).
  final List<String> _rowsJson;

  /// Each entry is a JSON-encoded row object (`{"col": value, ...}`).
  List<String> get rowsJson {
    if (_rowsJson is EqualUnmodifiableListView) return _rowsJson;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_rowsJson);
  }

  final int batchNum;
  final bool hasMore;
  final String status;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartChangeEvent_InitialDataBatchCopyWith<DartChangeEvent_InitialDataBatch>
      get copyWith => _$DartChangeEvent_InitialDataBatchCopyWithImpl<
          DartChangeEvent_InitialDataBatch>(this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartChangeEvent_InitialDataBatch &&
            (identical(other.subscriptionId, subscriptionId) ||
                other.subscriptionId == subscriptionId) &&
            const DeepCollectionEquality().equals(other._rowsJson, _rowsJson) &&
            (identical(other.batchNum, batchNum) ||
                other.batchNum == batchNum) &&
            (identical(other.hasMore, hasMore) || other.hasMore == hasMore) &&
            (identical(other.status, status) || other.status == status));
  }

  @override
  int get hashCode => Object.hash(
      runtimeType,
      subscriptionId,
      const DeepCollectionEquality().hash(_rowsJson),
      batchNum,
      hasMore,
      status);

  @override
  String toString() {
    return 'DartChangeEvent.initialDataBatch(subscriptionId: $subscriptionId, rowsJson: $rowsJson, batchNum: $batchNum, hasMore: $hasMore, status: $status)';
  }
}

/// @nodoc
abstract mixin class $DartChangeEvent_InitialDataBatchCopyWith<$Res>
    implements $DartChangeEventCopyWith<$Res> {
  factory $DartChangeEvent_InitialDataBatchCopyWith(
          DartChangeEvent_InitialDataBatch value,
          $Res Function(DartChangeEvent_InitialDataBatch) _then) =
      _$DartChangeEvent_InitialDataBatchCopyWithImpl;
  @override
  @useResult
  $Res call(
      {String subscriptionId,
      List<String> rowsJson,
      int batchNum,
      bool hasMore,
      String status});
}

/// @nodoc
class _$DartChangeEvent_InitialDataBatchCopyWithImpl<$Res>
    implements $DartChangeEvent_InitialDataBatchCopyWith<$Res> {
  _$DartChangeEvent_InitialDataBatchCopyWithImpl(this._self, this._then);

  final DartChangeEvent_InitialDataBatch _self;
  final $Res Function(DartChangeEvent_InitialDataBatch) _then;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @pragma('vm:prefer-inline')
  $Res call({
    Object? subscriptionId = null,
    Object? rowsJson = null,
    Object? batchNum = null,
    Object? hasMore = null,
    Object? status = null,
  }) {
    return _then(DartChangeEvent_InitialDataBatch(
      subscriptionId: null == subscriptionId
          ? _self.subscriptionId
          : subscriptionId // ignore: cast_nullable_to_non_nullable
              as String,
      rowsJson: null == rowsJson
          ? _self._rowsJson
          : rowsJson // ignore: cast_nullable_to_non_nullable
              as List<String>,
      batchNum: null == batchNum
          ? _self.batchNum
          : batchNum // ignore: cast_nullable_to_non_nullable
              as int,
      hasMore: null == hasMore
          ? _self.hasMore
          : hasMore // ignore: cast_nullable_to_non_nullable
              as bool,
      status: null == status
          ? _self.status
          : status // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class DartChangeEvent_Insert extends DartChangeEvent {
  const DartChangeEvent_Insert(
      {required this.subscriptionId, required final List<String> rowsJson})
      : _rowsJson = rowsJson,
        super._();

  @override
  final String subscriptionId;

  /// Each entry is a JSON-encoded row object.
  final List<String> _rowsJson;

  /// Each entry is a JSON-encoded row object.
  List<String> get rowsJson {
    if (_rowsJson is EqualUnmodifiableListView) return _rowsJson;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_rowsJson);
  }

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartChangeEvent_InsertCopyWith<DartChangeEvent_Insert> get copyWith =>
      _$DartChangeEvent_InsertCopyWithImpl<DartChangeEvent_Insert>(
          this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartChangeEvent_Insert &&
            (identical(other.subscriptionId, subscriptionId) ||
                other.subscriptionId == subscriptionId) &&
            const DeepCollectionEquality().equals(other._rowsJson, _rowsJson));
  }

  @override
  int get hashCode => Object.hash(runtimeType, subscriptionId,
      const DeepCollectionEquality().hash(_rowsJson));

  @override
  String toString() {
    return 'DartChangeEvent.insert(subscriptionId: $subscriptionId, rowsJson: $rowsJson)';
  }
}

/// @nodoc
abstract mixin class $DartChangeEvent_InsertCopyWith<$Res>
    implements $DartChangeEventCopyWith<$Res> {
  factory $DartChangeEvent_InsertCopyWith(DartChangeEvent_Insert value,
          $Res Function(DartChangeEvent_Insert) _then) =
      _$DartChangeEvent_InsertCopyWithImpl;
  @override
  @useResult
  $Res call({String subscriptionId, List<String> rowsJson});
}

/// @nodoc
class _$DartChangeEvent_InsertCopyWithImpl<$Res>
    implements $DartChangeEvent_InsertCopyWith<$Res> {
  _$DartChangeEvent_InsertCopyWithImpl(this._self, this._then);

  final DartChangeEvent_Insert _self;
  final $Res Function(DartChangeEvent_Insert) _then;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @pragma('vm:prefer-inline')
  $Res call({
    Object? subscriptionId = null,
    Object? rowsJson = null,
  }) {
    return _then(DartChangeEvent_Insert(
      subscriptionId: null == subscriptionId
          ? _self.subscriptionId
          : subscriptionId // ignore: cast_nullable_to_non_nullable
              as String,
      rowsJson: null == rowsJson
          ? _self._rowsJson
          : rowsJson // ignore: cast_nullable_to_non_nullable
              as List<String>,
    ));
  }
}

/// @nodoc

class DartChangeEvent_Update extends DartChangeEvent {
  const DartChangeEvent_Update(
      {required this.subscriptionId,
      required final List<String> rowsJson,
      required final List<String> oldRowsJson})
      : _rowsJson = rowsJson,
        _oldRowsJson = oldRowsJson,
        super._();

  @override
  final String subscriptionId;

  /// Delta rows — only changed columns + PK + `_seq`.
  /// Changed user columns are the non-system keys: filter by `!key.starts_with('_')`.
  final List<String> _rowsJson;

  /// Delta rows — only changed columns + PK + `_seq`.
  /// Changed user columns are the non-system keys: filter by `!key.starts_with('_')`.
  List<String> get rowsJson {
    if (_rowsJson is EqualUnmodifiableListView) return _rowsJson;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_rowsJson);
  }

  final List<String> _oldRowsJson;
  List<String> get oldRowsJson {
    if (_oldRowsJson is EqualUnmodifiableListView) return _oldRowsJson;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_oldRowsJson);
  }

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartChangeEvent_UpdateCopyWith<DartChangeEvent_Update> get copyWith =>
      _$DartChangeEvent_UpdateCopyWithImpl<DartChangeEvent_Update>(
          this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartChangeEvent_Update &&
            (identical(other.subscriptionId, subscriptionId) ||
                other.subscriptionId == subscriptionId) &&
            const DeepCollectionEquality().equals(other._rowsJson, _rowsJson) &&
            const DeepCollectionEquality()
                .equals(other._oldRowsJson, _oldRowsJson));
  }

  @override
  int get hashCode => Object.hash(
      runtimeType,
      subscriptionId,
      const DeepCollectionEquality().hash(_rowsJson),
      const DeepCollectionEquality().hash(_oldRowsJson));

  @override
  String toString() {
    return 'DartChangeEvent.update(subscriptionId: $subscriptionId, rowsJson: $rowsJson, oldRowsJson: $oldRowsJson)';
  }
}

/// @nodoc
abstract mixin class $DartChangeEvent_UpdateCopyWith<$Res>
    implements $DartChangeEventCopyWith<$Res> {
  factory $DartChangeEvent_UpdateCopyWith(DartChangeEvent_Update value,
          $Res Function(DartChangeEvent_Update) _then) =
      _$DartChangeEvent_UpdateCopyWithImpl;
  @override
  @useResult
  $Res call(
      {String subscriptionId, List<String> rowsJson, List<String> oldRowsJson});
}

/// @nodoc
class _$DartChangeEvent_UpdateCopyWithImpl<$Res>
    implements $DartChangeEvent_UpdateCopyWith<$Res> {
  _$DartChangeEvent_UpdateCopyWithImpl(this._self, this._then);

  final DartChangeEvent_Update _self;
  final $Res Function(DartChangeEvent_Update) _then;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @pragma('vm:prefer-inline')
  $Res call({
    Object? subscriptionId = null,
    Object? rowsJson = null,
    Object? oldRowsJson = null,
  }) {
    return _then(DartChangeEvent_Update(
      subscriptionId: null == subscriptionId
          ? _self.subscriptionId
          : subscriptionId // ignore: cast_nullable_to_non_nullable
              as String,
      rowsJson: null == rowsJson
          ? _self._rowsJson
          : rowsJson // ignore: cast_nullable_to_non_nullable
              as List<String>,
      oldRowsJson: null == oldRowsJson
          ? _self._oldRowsJson
          : oldRowsJson // ignore: cast_nullable_to_non_nullable
              as List<String>,
    ));
  }
}

/// @nodoc

class DartChangeEvent_Delete extends DartChangeEvent {
  const DartChangeEvent_Delete(
      {required this.subscriptionId, required final List<String> oldRowsJson})
      : _oldRowsJson = oldRowsJson,
        super._();

  @override
  final String subscriptionId;
  final List<String> _oldRowsJson;
  List<String> get oldRowsJson {
    if (_oldRowsJson is EqualUnmodifiableListView) return _oldRowsJson;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_oldRowsJson);
  }

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartChangeEvent_DeleteCopyWith<DartChangeEvent_Delete> get copyWith =>
      _$DartChangeEvent_DeleteCopyWithImpl<DartChangeEvent_Delete>(
          this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartChangeEvent_Delete &&
            (identical(other.subscriptionId, subscriptionId) ||
                other.subscriptionId == subscriptionId) &&
            const DeepCollectionEquality()
                .equals(other._oldRowsJson, _oldRowsJson));
  }

  @override
  int get hashCode => Object.hash(runtimeType, subscriptionId,
      const DeepCollectionEquality().hash(_oldRowsJson));

  @override
  String toString() {
    return 'DartChangeEvent.delete(subscriptionId: $subscriptionId, oldRowsJson: $oldRowsJson)';
  }
}

/// @nodoc
abstract mixin class $DartChangeEvent_DeleteCopyWith<$Res>
    implements $DartChangeEventCopyWith<$Res> {
  factory $DartChangeEvent_DeleteCopyWith(DartChangeEvent_Delete value,
          $Res Function(DartChangeEvent_Delete) _then) =
      _$DartChangeEvent_DeleteCopyWithImpl;
  @override
  @useResult
  $Res call({String subscriptionId, List<String> oldRowsJson});
}

/// @nodoc
class _$DartChangeEvent_DeleteCopyWithImpl<$Res>
    implements $DartChangeEvent_DeleteCopyWith<$Res> {
  _$DartChangeEvent_DeleteCopyWithImpl(this._self, this._then);

  final DartChangeEvent_Delete _self;
  final $Res Function(DartChangeEvent_Delete) _then;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @pragma('vm:prefer-inline')
  $Res call({
    Object? subscriptionId = null,
    Object? oldRowsJson = null,
  }) {
    return _then(DartChangeEvent_Delete(
      subscriptionId: null == subscriptionId
          ? _self.subscriptionId
          : subscriptionId // ignore: cast_nullable_to_non_nullable
              as String,
      oldRowsJson: null == oldRowsJson
          ? _self._oldRowsJson
          : oldRowsJson // ignore: cast_nullable_to_non_nullable
              as List<String>,
    ));
  }
}

/// @nodoc

class DartChangeEvent_Error extends DartChangeEvent {
  const DartChangeEvent_Error(
      {required this.subscriptionId, required this.code, required this.message})
      : super._();

  @override
  final String subscriptionId;
  final String code;
  final String message;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartChangeEvent_ErrorCopyWith<DartChangeEvent_Error> get copyWith =>
      _$DartChangeEvent_ErrorCopyWithImpl<DartChangeEvent_Error>(
          this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartChangeEvent_Error &&
            (identical(other.subscriptionId, subscriptionId) ||
                other.subscriptionId == subscriptionId) &&
            (identical(other.code, code) || other.code == code) &&
            (identical(other.message, message) || other.message == message));
  }

  @override
  int get hashCode => Object.hash(runtimeType, subscriptionId, code, message);

  @override
  String toString() {
    return 'DartChangeEvent.error(subscriptionId: $subscriptionId, code: $code, message: $message)';
  }
}

/// @nodoc
abstract mixin class $DartChangeEvent_ErrorCopyWith<$Res>
    implements $DartChangeEventCopyWith<$Res> {
  factory $DartChangeEvent_ErrorCopyWith(DartChangeEvent_Error value,
          $Res Function(DartChangeEvent_Error) _then) =
      _$DartChangeEvent_ErrorCopyWithImpl;
  @override
  @useResult
  $Res call({String subscriptionId, String code, String message});
}

/// @nodoc
class _$DartChangeEvent_ErrorCopyWithImpl<$Res>
    implements $DartChangeEvent_ErrorCopyWith<$Res> {
  _$DartChangeEvent_ErrorCopyWithImpl(this._self, this._then);

  final DartChangeEvent_Error _self;
  final $Res Function(DartChangeEvent_Error) _then;

  /// Create a copy of DartChangeEvent
  /// with the given fields replaced by the non-null parameter values.
  @override
  @pragma('vm:prefer-inline')
  $Res call({
    Object? subscriptionId = null,
    Object? code = null,
    Object? message = null,
  }) {
    return _then(DartChangeEvent_Error(
      subscriptionId: null == subscriptionId
          ? _self.subscriptionId
          : subscriptionId // ignore: cast_nullable_to_non_nullable
              as String,
      code: null == code
          ? _self.code
          : code // ignore: cast_nullable_to_non_nullable
              as String,
      message: null == message
          ? _self.message
          : message // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc
mixin _$DartConnectionEvent {
  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType && other is DartConnectionEvent);
  }

  @override
  int get hashCode => runtimeType.hashCode;

  @override
  String toString() {
    return 'DartConnectionEvent()';
  }
}

/// @nodoc
class $DartConnectionEventCopyWith<$Res> {
  $DartConnectionEventCopyWith(
      DartConnectionEvent _, $Res Function(DartConnectionEvent) __);
}

/// Adds pattern-matching-related methods to [DartConnectionEvent].
extension DartConnectionEventPatterns on DartConnectionEvent {
  /// A variant of `map` that fallback to returning `orElse`.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case _:
  ///     return orElse();
  /// }
  /// ```

  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(DartConnectionEvent_Connect value)? connect,
    TResult Function(DartConnectionEvent_Disconnect value)? disconnect,
    TResult Function(DartConnectionEvent_Error value)? error,
    TResult Function(DartConnectionEvent_Receive value)? receive,
    TResult Function(DartConnectionEvent_Send value)? send,
    required TResult orElse(),
  }) {
    final _that = this;
    switch (_that) {
      case DartConnectionEvent_Connect() when connect != null:
        return connect(_that);
      case DartConnectionEvent_Disconnect() when disconnect != null:
        return disconnect(_that);
      case DartConnectionEvent_Error() when error != null:
        return error(_that);
      case DartConnectionEvent_Receive() when receive != null:
        return receive(_that);
      case DartConnectionEvent_Send() when send != null:
        return send(_that);
      case _:
        return orElse();
    }
  }

  /// A `switch`-like method, using callbacks.
  ///
  /// Callbacks receives the raw object, upcasted.
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case final Subclass2 value:
  ///     return ...;
  /// }
  /// ```

  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(DartConnectionEvent_Connect value) connect,
    required TResult Function(DartConnectionEvent_Disconnect value) disconnect,
    required TResult Function(DartConnectionEvent_Error value) error,
    required TResult Function(DartConnectionEvent_Receive value) receive,
    required TResult Function(DartConnectionEvent_Send value) send,
  }) {
    final _that = this;
    switch (_that) {
      case DartConnectionEvent_Connect():
        return connect(_that);
      case DartConnectionEvent_Disconnect():
        return disconnect(_that);
      case DartConnectionEvent_Error():
        return error(_that);
      case DartConnectionEvent_Receive():
        return receive(_that);
      case DartConnectionEvent_Send():
        return send(_that);
    }
  }

  /// A variant of `map` that fallback to returning `null`.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case final Subclass value:
  ///     return ...;
  ///   case _:
  ///     return null;
  /// }
  /// ```

  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(DartConnectionEvent_Connect value)? connect,
    TResult? Function(DartConnectionEvent_Disconnect value)? disconnect,
    TResult? Function(DartConnectionEvent_Error value)? error,
    TResult? Function(DartConnectionEvent_Receive value)? receive,
    TResult? Function(DartConnectionEvent_Send value)? send,
  }) {
    final _that = this;
    switch (_that) {
      case DartConnectionEvent_Connect() when connect != null:
        return connect(_that);
      case DartConnectionEvent_Disconnect() when disconnect != null:
        return disconnect(_that);
      case DartConnectionEvent_Error() when error != null:
        return error(_that);
      case DartConnectionEvent_Receive() when receive != null:
        return receive(_that);
      case DartConnectionEvent_Send() when send != null:
        return send(_that);
      case _:
        return null;
    }
  }

  /// A variant of `when` that fallback to an `orElse` callback.
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case _:
  ///     return orElse();
  /// }
  /// ```

  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function()? connect,
    TResult Function(DartDisconnectReason reason)? disconnect,
    TResult Function(DartConnectionError error)? error,
    TResult Function(String message)? receive,
    TResult Function(String message)? send,
    required TResult orElse(),
  }) {
    final _that = this;
    switch (_that) {
      case DartConnectionEvent_Connect() when connect != null:
        return connect();
      case DartConnectionEvent_Disconnect() when disconnect != null:
        return disconnect(_that.reason);
      case DartConnectionEvent_Error() when error != null:
        return error(_that.error);
      case DartConnectionEvent_Receive() when receive != null:
        return receive(_that.message);
      case DartConnectionEvent_Send() when send != null:
        return send(_that.message);
      case _:
        return orElse();
    }
  }

  /// A `switch`-like method, using callbacks.
  ///
  /// As opposed to `map`, this offers destructuring.
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case Subclass2(:final field2):
  ///     return ...;
  /// }
  /// ```

  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function() connect,
    required TResult Function(DartDisconnectReason reason) disconnect,
    required TResult Function(DartConnectionError error) error,
    required TResult Function(String message) receive,
    required TResult Function(String message) send,
  }) {
    final _that = this;
    switch (_that) {
      case DartConnectionEvent_Connect():
        return connect();
      case DartConnectionEvent_Disconnect():
        return disconnect(_that.reason);
      case DartConnectionEvent_Error():
        return error(_that.error);
      case DartConnectionEvent_Receive():
        return receive(_that.message);
      case DartConnectionEvent_Send():
        return send(_that.message);
    }
  }

  /// A variant of `when` that fallback to returning `null`
  ///
  /// It is equivalent to doing:
  /// ```dart
  /// switch (sealedClass) {
  ///   case Subclass(:final field):
  ///     return ...;
  ///   case _:
  ///     return null;
  /// }
  /// ```

  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function()? connect,
    TResult? Function(DartDisconnectReason reason)? disconnect,
    TResult? Function(DartConnectionError error)? error,
    TResult? Function(String message)? receive,
    TResult? Function(String message)? send,
  }) {
    final _that = this;
    switch (_that) {
      case DartConnectionEvent_Connect() when connect != null:
        return connect();
      case DartConnectionEvent_Disconnect() when disconnect != null:
        return disconnect(_that.reason);
      case DartConnectionEvent_Error() when error != null:
        return error(_that.error);
      case DartConnectionEvent_Receive() when receive != null:
        return receive(_that.message);
      case DartConnectionEvent_Send() when send != null:
        return send(_that.message);
      case _:
        return null;
    }
  }
}

/// @nodoc

class DartConnectionEvent_Connect extends DartConnectionEvent {
  const DartConnectionEvent_Connect() : super._();

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartConnectionEvent_Connect);
  }

  @override
  int get hashCode => runtimeType.hashCode;

  @override
  String toString() {
    return 'DartConnectionEvent.connect()';
  }
}

/// @nodoc

class DartConnectionEvent_Disconnect extends DartConnectionEvent {
  const DartConnectionEvent_Disconnect({required this.reason}) : super._();

  final DartDisconnectReason reason;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartConnectionEvent_DisconnectCopyWith<DartConnectionEvent_Disconnect>
      get copyWith => _$DartConnectionEvent_DisconnectCopyWithImpl<
          DartConnectionEvent_Disconnect>(this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartConnectionEvent_Disconnect &&
            (identical(other.reason, reason) || other.reason == reason));
  }

  @override
  int get hashCode => Object.hash(runtimeType, reason);

  @override
  String toString() {
    return 'DartConnectionEvent.disconnect(reason: $reason)';
  }
}

/// @nodoc
abstract mixin class $DartConnectionEvent_DisconnectCopyWith<$Res>
    implements $DartConnectionEventCopyWith<$Res> {
  factory $DartConnectionEvent_DisconnectCopyWith(
          DartConnectionEvent_Disconnect value,
          $Res Function(DartConnectionEvent_Disconnect) _then) =
      _$DartConnectionEvent_DisconnectCopyWithImpl;
  @useResult
  $Res call({DartDisconnectReason reason});
}

/// @nodoc
class _$DartConnectionEvent_DisconnectCopyWithImpl<$Res>
    implements $DartConnectionEvent_DisconnectCopyWith<$Res> {
  _$DartConnectionEvent_DisconnectCopyWithImpl(this._self, this._then);

  final DartConnectionEvent_Disconnect _self;
  final $Res Function(DartConnectionEvent_Disconnect) _then;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  $Res call({
    Object? reason = null,
  }) {
    return _then(DartConnectionEvent_Disconnect(
      reason: null == reason
          ? _self.reason
          : reason // ignore: cast_nullable_to_non_nullable
              as DartDisconnectReason,
    ));
  }
}

/// @nodoc

class DartConnectionEvent_Error extends DartConnectionEvent {
  const DartConnectionEvent_Error({required this.error}) : super._();

  final DartConnectionError error;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartConnectionEvent_ErrorCopyWith<DartConnectionEvent_Error> get copyWith =>
      _$DartConnectionEvent_ErrorCopyWithImpl<DartConnectionEvent_Error>(
          this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartConnectionEvent_Error &&
            (identical(other.error, error) || other.error == error));
  }

  @override
  int get hashCode => Object.hash(runtimeType, error);

  @override
  String toString() {
    return 'DartConnectionEvent.error(error: $error)';
  }
}

/// @nodoc
abstract mixin class $DartConnectionEvent_ErrorCopyWith<$Res>
    implements $DartConnectionEventCopyWith<$Res> {
  factory $DartConnectionEvent_ErrorCopyWith(DartConnectionEvent_Error value,
          $Res Function(DartConnectionEvent_Error) _then) =
      _$DartConnectionEvent_ErrorCopyWithImpl;
  @useResult
  $Res call({DartConnectionError error});
}

/// @nodoc
class _$DartConnectionEvent_ErrorCopyWithImpl<$Res>
    implements $DartConnectionEvent_ErrorCopyWith<$Res> {
  _$DartConnectionEvent_ErrorCopyWithImpl(this._self, this._then);

  final DartConnectionEvent_Error _self;
  final $Res Function(DartConnectionEvent_Error) _then;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  $Res call({
    Object? error = null,
  }) {
    return _then(DartConnectionEvent_Error(
      error: null == error
          ? _self.error
          : error // ignore: cast_nullable_to_non_nullable
              as DartConnectionError,
    ));
  }
}

/// @nodoc

class DartConnectionEvent_Receive extends DartConnectionEvent {
  const DartConnectionEvent_Receive({required this.message}) : super._();

  final String message;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartConnectionEvent_ReceiveCopyWith<DartConnectionEvent_Receive>
      get copyWith => _$DartConnectionEvent_ReceiveCopyWithImpl<
          DartConnectionEvent_Receive>(this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartConnectionEvent_Receive &&
            (identical(other.message, message) || other.message == message));
  }

  @override
  int get hashCode => Object.hash(runtimeType, message);

  @override
  String toString() {
    return 'DartConnectionEvent.receive(message: $message)';
  }
}

/// @nodoc
abstract mixin class $DartConnectionEvent_ReceiveCopyWith<$Res>
    implements $DartConnectionEventCopyWith<$Res> {
  factory $DartConnectionEvent_ReceiveCopyWith(
          DartConnectionEvent_Receive value,
          $Res Function(DartConnectionEvent_Receive) _then) =
      _$DartConnectionEvent_ReceiveCopyWithImpl;
  @useResult
  $Res call({String message});
}

/// @nodoc
class _$DartConnectionEvent_ReceiveCopyWithImpl<$Res>
    implements $DartConnectionEvent_ReceiveCopyWith<$Res> {
  _$DartConnectionEvent_ReceiveCopyWithImpl(this._self, this._then);

  final DartConnectionEvent_Receive _self;
  final $Res Function(DartConnectionEvent_Receive) _then;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  $Res call({
    Object? message = null,
  }) {
    return _then(DartConnectionEvent_Receive(
      message: null == message
          ? _self.message
          : message // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class DartConnectionEvent_Send extends DartConnectionEvent {
  const DartConnectionEvent_Send({required this.message}) : super._();

  final String message;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @pragma('vm:prefer-inline')
  $DartConnectionEvent_SendCopyWith<DartConnectionEvent_Send> get copyWith =>
      _$DartConnectionEvent_SendCopyWithImpl<DartConnectionEvent_Send>(
          this, _$identity);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is DartConnectionEvent_Send &&
            (identical(other.message, message) || other.message == message));
  }

  @override
  int get hashCode => Object.hash(runtimeType, message);

  @override
  String toString() {
    return 'DartConnectionEvent.send(message: $message)';
  }
}

/// @nodoc
abstract mixin class $DartConnectionEvent_SendCopyWith<$Res>
    implements $DartConnectionEventCopyWith<$Res> {
  factory $DartConnectionEvent_SendCopyWith(DartConnectionEvent_Send value,
          $Res Function(DartConnectionEvent_Send) _then) =
      _$DartConnectionEvent_SendCopyWithImpl;
  @useResult
  $Res call({String message});
}

/// @nodoc
class _$DartConnectionEvent_SendCopyWithImpl<$Res>
    implements $DartConnectionEvent_SendCopyWith<$Res> {
  _$DartConnectionEvent_SendCopyWithImpl(this._self, this._then);

  final DartConnectionEvent_Send _self;
  final $Res Function(DartConnectionEvent_Send) _then;

  /// Create a copy of DartConnectionEvent
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  $Res call({
    Object? message = null,
  }) {
    return _then(DartConnectionEvent_Send(
      message: null == message
          ? _self.message
          : message // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

// dart format on
