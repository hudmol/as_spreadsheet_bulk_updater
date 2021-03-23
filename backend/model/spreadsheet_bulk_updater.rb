class SpreadsheetBulkUpdater

  extend JSONModel

  def self.run(filename, job)
    check_sheet(filename)

    pp "DO SOMETHING!"

    # FIXME
    {
      updated: 0,
    }
  end

  def self.check_sheet(filename)
    pp "TODO something clever"
  end

  class SpreadsheetBulkUpdateFailed < StandardError
    attr_reader :errors

    def initialize(errors)
      @errors = errors
    end

    def to_json
      @errors
    end
  end

  Row = Struct.new(:values, :row_number) do
    def has_heading?(column)
      self.values.include?(column.heading)
    end

    def fetch(*args)
      args[0] = args[0].heading

      self.values.fetch(*args)
    end

    def empty?
      values.all?{|_, v| v.to_s.strip.empty?}
    end
  end

end
