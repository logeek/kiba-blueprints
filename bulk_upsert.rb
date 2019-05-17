require 'kiba'
require 'kiba-common/sources/enumerable'
require 'kiba-pro/destinations/sql_bulk_insert'
require 'sequel'
require_relative 'dsl_extensions/feedback'

module Kiba
  module Blueprints
    module BulkUpsert
      module_function

      def setup(database:, base_price:, description_prefix:, count: 100_000)
        klass = self

        Kiba.parse do
          extend Kiba::Blueprints::DSLExtensions::Feedback
          setup_feedback(klass)

          source Kiba::Common::Sources::Enumerable, (1..count)
          
          # NOTE: we use this cheap technique to ensure non regression of the blueprint.
          # This code is not necessarily a recommendation of how to implement that.
          post_process do
            # just a couple of sanity checks here 
            fail "Invalid number of records" unless database[:products].count == count
            test_record = database[:products].where(product_ref: "ref-1").first!
            fail "Invalid test record (#{test_record})" unless test_record.slice(:description, :price_cents) == {
              description: description_prefix + " (1)",
              price_cents: base_price + 1
            }
            logger.info "Sanity checks OK!"
          end

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
