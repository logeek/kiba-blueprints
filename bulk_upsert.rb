require 'kiba'
require 'kiba-common/sources/enumerable'
require 'kiba-pro/destinations/sql_bulk_insert'
require 'sequel'
require_relative 'dsl_extensions/feedback'

module Kiba
  module Blueprints
    module BulkUpsert
      module_function

      def setup(database:, base_price:, description_prefix:)
        klass = self

        Kiba.parse do
          extend Kiba::Blueprints::DSLExtensions::Feedback
          setup_feedback(klass)

          source Kiba::Common::Sources::Enumerable, (1..100_000)

          transform { |r|
            {
              product_ref: "ref-#{r}",
              price_cents: base_price + r,
              quantity: 5_000,
              description: description_prefix + " (#{r})"
            }
          }

          destination Kiba::Pro::Destinations::SQLBulkInsert,
            database: database,
            table: :products,
            buffer_size: 25_000,
            dataset: -> (dataset) {
              dataset.insert_conflict(
                target: :product_ref,
                update: {
                  price_cents: Sequel[:excluded][:price_cents],
                  quantity: Sequel[:excluded][:quantity],
                  description: Sequel[:excluded][:description]
                }
              )
            }
        end
      end
    end
  end
end
