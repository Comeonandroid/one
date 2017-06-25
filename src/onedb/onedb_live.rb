
require 'opennebula'

class OneDBLive
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

    def delete_sql(table, where)
        "DELETE from #{table} WHERE #{where}"
    end

    def delete(table, where, federate)
        sql = delete_sql(table, where)
        db_exec(sql, "Error deleting record", federate)
    end

    def update_body_sql(table, body, where)
        "UPDATE vm_pool SET body = '#{db_escape(body)}' WHERE #{where}"
    end

    def update_body(table, body, where, federate)
        sql = update_body_sql(table, body, where)
        db_exec(sql, "Error updating record", federate)
    end

    def db_exec(sql, error_msg, federate = false)
        rc = system.sql_command(sql, federate)
        if OpenNebula.is_error?(rc)
            raise "#{error_msg}: #{rc.message}"
        end
    end

    def purge_history
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

                # Update VM body to leave only the last history record
                body = db_escape(vm.to_xml)
                update_body("vm_pool", vm.to_xml, "oid = #{vm.id}", false)

                # Delete any history record that does not have the same
                # SEQ number as the last history record
                seq_num = last_history['SEQ']
                delete("history", "vid = #{vm.id} and seq != #{seq_num}", false)
            end
        end
    end

    def purge_done_vm
        vmpool = OpenNebula::VirtualMachinePool.new(client)
        vmpool.info(OpenNebula::Pool::INFO_ALL,
                    -1,
                    -1,
                    OpenNebula::VirtualMachine::VM_STATE.index('DONE'))

        vmpool.each do |vm|
            delete("vm_pool", "oid = #{vm.id}", false)
            delete("history", "vid = #{vm.id}", false)
        end
    end
end
