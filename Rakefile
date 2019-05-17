desc "Bulk upserts demonstration (tested with PG)"
task :bulk_upsert do
  require_relative 'bulk_upsert'
  options = {}
  if ENV['USE_DB_LOGGER'] == '1'
    options[:logger] = Logger.new(STDOUT)
  end
  Sequel.connect(ENV.fetch('DATABASE_URL'), options) do |db|
    db.create_table! :products do
      primary_key :id
      # NOTE: this does not create an index ; this is
      # actually slower with an index.
      column :product_ref, :text, unique: true
      column :price_cents, :integer
      column :quantity, :integer
      column :description, :text
    end
    # Since we're doing a bulk upsert here, we'll do multiple runs
    [
      {base_price: 100, description_prefix: "First run description"},
      {base_price: 200, description_prefix: "Second run description"}
    ].each do |options|
      Kiba.run(Kiba::Blueprints::BulkUpsert.setup(
        options.merge(database: db, count: 100_000)
      ))
    end
  end
end
