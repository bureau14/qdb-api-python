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

#include "arrow/engine/substrait/plan_internal.h"

#include "arrow/result.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/unreachable.h"

#include <unordered_map>

namespace arrow {

using internal::checked_cast;

namespace engine {

namespace internal {
using ::arrow::internal::make_unique;
}  // namespace internal

Status AddExtensionSetToPlan(const ExtensionSet& ext_set, substrait::Plan* plan) {
  plan->clear_extension_uris();

  std::unordered_map<util::string_view, int, ::arrow::internal::StringViewHash> map;

  auto uris = plan->mutable_extension_uris();
  uris->Reserve(static_cast<int>(ext_set.uris().size()));
  for (uint32_t anchor = 0; anchor < ext_set.uris().size(); ++anchor) {
    auto uri = ext_set.uris()[anchor];
    if (uri.empty()) continue;

    auto ext_uri = internal::make_unique<substrait::extensions::SimpleExtensionURI>();
    ext_uri->set_uri(uri.to_string());
    ext_uri->set_extension_uri_anchor(anchor);
    uris->AddAllocated(ext_uri.release());

    map[uri] = anchor;
  }

  auto extensions = plan->mutable_extensions();
  extensions->Reserve(static_cast<int>(ext_set.num_types() + ext_set.num_functions()));

  using ExtDecl = substrait::extensions::SimpleExtensionDeclaration;

  for (uint32_t anchor = 0; anchor < ext_set.num_types(); ++anchor) {
    ARROW_ASSIGN_OR_RAISE(auto type_record, ext_set.DecodeType(anchor));
    if (type_record.id.empty()) continue;

    auto ext_decl = internal::make_unique<ExtDecl>();

    if (type_record.is_variation) {
      auto type_var = internal::make_unique<ExtDecl::ExtensionTypeVariation>();
      type_var->set_extension_uri_reference(map[type_record.id.uri]);
      type_var->set_type_variation_anchor(anchor);
      type_var->set_name(type_record.id.name.to_string());
      ext_decl->set_allocated_extension_type_variation(type_var.release());
    } else {
      auto type = internal::make_unique<ExtDecl::ExtensionType>();
      type->set_extension_uri_reference(map[type_record.id.uri]);
      type->set_type_anchor(anchor);
      type->set_name(type_record.id.name.to_string());
      ext_decl->set_allocated_extension_type(type.release());
    }

    extensions->AddAllocated(ext_decl.release());
  }

  for (uint32_t anchor = 0; anchor < ext_set.num_functions(); ++anchor) {
    ARROW_ASSIGN_OR_RAISE(auto function_record, ext_set.DecodeFunction(anchor));
    if (function_record.id.empty()) continue;

    auto fn = internal::make_unique<ExtDecl::ExtensionFunction>();
    fn->set_extension_uri_reference(map[function_record.id.uri]);
    fn->set_function_anchor(anchor);
    fn->set_name(function_record.id.name.to_string());

    auto ext_decl = internal::make_unique<ExtDecl>();
    ext_decl->set_allocated_extension_function(fn.release());
    extensions->AddAllocated(ext_decl.release());
  }

  return Status::OK();
}

namespace {
template <typename Element, typename T>
void SetElement(size_t i, const Element& element, std::vector<T>* vector) {
  DCHECK_LE(i, 1 << 20);
  if (i >= vector->size()) {
    vector->resize(i + 1);
  }
  (*vector)[i] = static_cast<T>(element);
}
}  // namespace

Result<ExtensionSet> GetExtensionSetFromPlan(const substrait::Plan& plan,
                                             ExtensionIdRegistry* registry) {
  std::vector<util::string_view> uris;
  for (const auto& uri : plan.extension_uris()) {
    SetElement(uri.extension_uri_anchor(), uri.uri(), &uris);
  }

  // NOTE: it's acceptable to use views to memory owned by plan; ExtensionSet::Make
  // will only store views to memory owned by registry.

  using Id = ExtensionSet::Id;

  std::vector<Id> type_ids, function_ids;
  std::vector<bool> type_is_variation;
  for (const auto& ext : plan.extensions()) {
    switch (ext.mapping_type_case()) {
      case substrait::extensions::SimpleExtensionDeclaration::kExtensionTypeVariation: {
        const auto& type_var = ext.extension_type_variation();
        util::string_view uri = uris[type_var.extension_uri_reference()];
        SetElement(type_var.type_variation_anchor(), Id{uri, type_var.name()}, &type_ids);
        SetElement(type_var.type_variation_anchor(), true, &type_is_variation);
        break;
      }

      case substrait::extensions::SimpleExtensionDeclaration::kExtensionType: {
        const auto& type = ext.extension_type();
        util::string_view uri = uris[type.extension_uri_reference()];
        SetElement(type.type_anchor(), Id{uri, type.name()}, &type_ids);
        SetElement(type.type_anchor(), false, &type_is_variation);
        break;
      }

      case substrait::extensions::SimpleExtensionDeclaration::kExtensionFunction: {
        const auto& fn = ext.extension_function();
        util::string_view uri = uris[fn.extension_uri_reference()];
        SetElement(fn.function_anchor(), Id{uri, fn.name()}, &function_ids);
        break;
      }

      default:
        Unreachable();
    }
  }

  return ExtensionSet::Make(std::move(uris), std::move(type_ids),
                            std::move(type_is_variation), std::move(function_ids),
                            registry);
}

}  // namespace engine
}  // namespace arrow
