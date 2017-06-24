
require 'opennebula'

class OneDBAction
    class Base
        def initialize
            @client = nil
            @system = nil
        end

        def client
            @client ||= OpenNebula::Client.new
        end

        def system
            @system ||= OpenNebula::System.new(client)
        end

        def db_escape(string)
            string.gsub("'", "''")
        end
    end
end

class OneDBAction::PurgeHistory < OneDBAction::Base
    def run
        vmpool = OpenNebula::VirtualMachinePool.new(client)
        vmpool.info_all

        vmpool.each do |vm|
            # vmpool info only returns the last history record. We can check
            # if this VM can have more than one record using the sequence
            # number. If it's 0 or it does not exist we can skip the VM.
            # Also take tone that xpaths on VM info that comes from VMPool
            # or VM is different. We can not use absolute searches with
            # objects coming from pool.
            seq = vm['HISTORY_RECORDS/HISTORY/SEQ']
            next if !seq || seq == '0'

            # If the history can contain more than one record we get
            # all the info for two reasons:
            #
            #   * Make sure that all the info is written back
            #   * Refresh the information so it's less probable that the info
            #     was modified during this process
            vm.info

            hash = vm.to_hash
            val_history = hash['VM']['HISTORY_RECORDS']['HISTORY']

            if Array === val_history && val_history.size > 1
                last_history = val_history.last
                vm.delete_element('HISTORY_RECORDS/HISTORY')
                vm.add_element('HISTORY_RECORDS', 'HISTORY' => last_history)

                body = db_escape(vm.to_xml)
                sql = "UPDATE vm_pool SET body = '#{body}' WHERE oid = #{vm.id}"

                rc = system.sql_command(sql, false)
                if OpenNebula.is_error?(rc)
                    raise "Error updating record: #{rc.message}"
                end
            end
        end

        0
    end
end
