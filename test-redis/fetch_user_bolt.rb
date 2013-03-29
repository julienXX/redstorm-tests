require 'red_storm'
require 'koala'

module RedStorm
  module Examples
    class FetchUserBolt < RedStorm::SimpleBolt
      output_fields :uid, :name

      on_init do
        @graph = Koala::Facebook::API.new("AAACAq9RHZA08BAK0v8gcRZAG4s7SpmpO410AQ6Omg1ut8SkdMdXuOH85CnK3dGJS3W3hFCx7YVLmTdPzc524f6KllEKDn6ZCpUctQLbxQZDZD")
      end

      # block declaration style using auto-emit (default)
      #
      on_receive do |tuple|
        uid = tuple.getValue(0).to_s
        begin
          user = @graph.get_object(uid)
          name = user["name"]
        rescue
          name = "undefined"
        end

        [uid, name]
      end
    end
  end
end
