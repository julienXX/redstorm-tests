require 'red_storm'
require 'massive_record'

module RedStorm
  module Examples
    class SendToHbaseBolt < RedStorm::SimpleBolt
      output_fields :name

      on_init do
        hbase_connection
      end

      # block declaration style using auto-emit (default)
      #
      on_receive do |tuple|
        uid = tuple.getValue(0).to_s
        name = tuple.getValue(1).to_s
        row = MassiveRecord::Wrapper::Row.new
        row.id = uid
        row.values = {user: {name: name }}
        row.table = @table
        row.save
        [name]
      end

      private

      def hbase_connection
        @hbase = MassiveRecord::Wrapper::Connection.new(:host => 'ec2-50-16-149-159.compute-1.amazonaws.com', :port => 9090)
        @hbase.open
        @table = MassiveRecord::Wrapper::Table.new(@hbase, :facebook)
      end
    end
  end
end
