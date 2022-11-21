// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This API is EXPERIMENTAL.

#pragma once

#include <vector>

#include "arrow/engine/substrait/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace engine {

/// Substrait identifies functions and custom data types using a (uri, name) pair.
///
/// This registry is a bidirectional mapping between Substrait IDs and their corresponding
/// Arrow counterparts (arrow::DataType and function names in a function registry)
///
/// Substrait extension types and variations must be registered with their corresponding
/// arrow::DataType before they can be used!
///
/// Conceptually this can be thought of as two pairs of `unordered_map`s.  One pair to
/// go back and forth between Substrait ID and arrow::DataType and another pair to go
/// back and forth between Substrait ID and Arrow function names.
///
/// Unlike an ExtensionSet this registry is not created automatically when consuming
/// Substrait plans and must be configured ahead of time (although there is a default
/// instance).
class ARROW_ENGINE_EXPORT ExtensionIdRegistry {
 public:
  /// All uris registered in this ExtensionIdRegistry
  virtual std::vector<util::string_view> Uris() const = 0;

  struct Id {
    util::string_view uri, name;

    bool empty() const { return uri.empty() && name.empty(); }
  };

  /// \brief A mapping between a Substrait ID and an arrow::DataType
  struct TypeRecord {
    Id id;
    const std::shared_ptr<DataType>& type;
    bool is_variation;
  };
  virtual util::optional<TypeRecord> GetType(const DataType&) const = 0;
  virtual util::optional<TypeRecord> GetType(Id, bool is_variation) const = 0;
  virtual Status RegisterType(Id, std::shared_ptr<DataType>, bool is_variation) = 0;

  /// \brief A mapping between a Substrait ID and an Arrow function
  ///
  /// Note: At the moment we identify functions solely by the name
  /// of the function in the function registry.
  ///
  /// TODO(ARROW-15582) some functions will not be simple enough to convert without access
  /// to their arguments/options. For example is_in embeds the set in options rather than
  /// using an argument:
  ///     is_in(x, SetLookupOptions(set)) <-> (k...Uri, "is_in")(x, set)
  ///
  /// ... for another example, depending on the value of the first argument to
  /// substrait::add it either corresponds to arrow::add or arrow::add_checked
  struct FunctionRecord {
    Id id;
    const std::string& function_name;
  };
  virtual util::optional<FunctionRecord> GetFunction(Id) const = 0;
  virtual util::optional<FunctionRecord> GetFunction(
      util::string_view arrow_function_name) const = 0;
  virtual Status RegisterFunction(Id, std::string arrow_function_name) = 0;
};

constexpr util::string_view kArrowExtTypesUri =
    "https://github.com/apache/arrow/blob/master/format/substrait/"
    "extension_types.yaml";

/// A default registry with all supported functions and data types registered
///
/// Note: Function support is currently very minimal, see ARROW-15538
ARROW_ENGINE_EXPORT ExtensionIdRegistry* default_extension_id_registry();

/// \brief A set of extensions used within a plan
///
/// Each time an extension is used within a Substrait plan the extension
/// must be included in an extension set that is defined at the root of the
/// plan.
///
/// The plan refers to a specific extension using an "anchor" which is an
/// arbitrary integer invented by the producer that has no meaning beyond a
/// plan but which should be consistent within a plan.
///
/// To support serialization and deserialization this type serves as a
/// bidirectional map between Substrait ID and "anchor"s.
///
/// When deserializing a Substrait plan the extension set should be extracted
/// after the plan has been converted from Protobuf and before the plan
/// is converted to an execution plan.
///
/// The extension set can be kept and reused during serialization if a perfect
/// round trip is required.  If serialization is not needed or round tripping
/// is not required then the extension set can be safely discarded after the
/// plan has been converted into an execution plan.
///
/// When converting an execution plan into a Substrait plan an extension set
/// can be automatically generated or a previously generated extension set can
/// be used.
///
/// ExtensionSet does not own strings; it only refers to strings in an
/// ExtensionIdRegistry.
class ARROW_ENGINE_EXPORT ExtensionSet {
 public:
  using Id = ExtensionIdRegistry::Id;

  struct FunctionRecord {
    Id id;
    util::string_view name;
  };

  struct TypeRecord {
    Id id;
    std::shared_ptr<DataType> type;
    bool is_variation;
  };

  /// Construct an empty ExtensionSet to be populated during serialization.
  explicit ExtensionSet(ExtensionIdRegistry* = default_extension_id_registry());
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExtensionSet);

  /// Construct an ExtensionSet with explicit extension ids for efficient referencing
  /// during deserialization. Note that input vectors need not be densely packed; an empty
  /// (default constructed) Id may be used as a placeholder to indicate an unused
  /// _anchor/_reference. This factory will be used to wrap the extensions declared in a
  /// substrait::Plan before deserializing the plan's relations.
  ///
  /// Views will be replaced with equivalent views pointing to memory owned by the
  /// registry.
  ///
  /// Note: This is an advanced operation.  The order of the ids, types, and functions
  /// must match the anchor numbers chosen for a plan.
  ///
  /// An extension set should instead be created using
  /// arrow::engine::GetExtensionSetFromPlan
  static Result<ExtensionSet> Make(
      std::vector<util::string_view> uris, std::vector<Id> type_ids,
      std::vector<bool> type_is_variation, std::vector<Id> function_ids,
      ExtensionIdRegistry* = default_extension_id_registry());

  // index in these vectors == value of _anchor/_reference fields
  /// TODO(ARROW-15583) this assumes that _anchor/_references won't be huge, which is not
  /// guaranteed. Could it be?
  const std::vector<util::string_view>& uris() const { return uris_; }

  /// \brief Returns a data type given an anchor
  ///
  /// This is used when converting a Substrait plan to an Arrow execution plan.
  ///
  /// If the anchor does not exist in this extension set an error will be returned.
  Result<TypeRecord> DecodeType(uint32_t anchor) const;

  /// \brief Returns the number of custom type records in this extension set
  ///
  /// Note: the types are currently stored as a sparse vector, so this may return a value
  /// larger than the actual number of types. This behavior may change in the future; see
  /// ARROW-15583.
  std::size_t num_types() const { return types_.size(); }

  /// \brief Lookup the anchor for a given type
  ///
  /// This operation is used when converting an Arrow execution plan to a Substrait plan.
  /// If the type has been previously encoded then the same anchor value will returned.
  ///
  /// If the type has not been previously encoded then a new anchor value will be created.
  ///
  /// If the type does not exist in the extension id registry then an error will be
  /// returned.
  ///
  /// \return An anchor that can be used to refer to the type within a plan
  Result<uint32_t> EncodeType(const DataType& type);

  /// \brief Returns a function given an anchor
  ///
  /// This is used when converting a Substrait plan to an Arrow execution plan.
  ///
  /// If the anchor does not exist in this extension set an error will be returned.
  Result<FunctionRecord> DecodeFunction(uint32_t anchor) const;

  /// \brief Lookup the anchor for a given function
  ///
  /// This operation is used when converting an Arrow execution plan to a Substrait  plan.
  /// If the function has been previously encoded then the same anchor value will be
  /// returned.
  ///
  /// If the function has not been previously encoded then a new anchor value will be
  /// created.
  ///
  /// If the function name is not in the extension id registry then an error will be
  /// returned.
  ///
  /// \return An anchor that can be used to refer to the function within a plan
  Result<uint32_t> EncodeFunction(util::string_view function_name);

  /// \brief Returns the number of custom functions in this extension set
  ///
  /// Note: the functions are currently stored as a sparse vector, so this may return a
  /// value larger than the actual number of functions. This behavior may change in the
  /// future; see ARROW-15583.
  std::size_t num_functions() const { return functions_.size(); }

 private:
  ExtensionIdRegistry* registry_;
  /// The subset of extension registry URIs referenced by this extension set
  std::vector<util::string_view> uris_;
  std::vector<TypeRecord> types_;

  std::vector<FunctionRecord> functions_;

  // pimpl pattern to hide lookup details
  struct Impl;
  std::unique_ptr<Impl, void (*)(Impl*)> impl_;
};

}  // namespace engine
}  // namespace arrow
