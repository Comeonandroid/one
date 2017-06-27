
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

    def update_sql(table, values, where)
        str = "UPDATE #{table} SET "

        changes = []

        values.each do |key, value|
            change = "#{key.to_s} = "

            case value
            when String, Symbol
                change << "'#{db_escape(value.to_s)}'"
            when Numeric
                change << value.to_s
            else
                change << value.to_s
            end

            changes << change
        end

        str << changes.join(', ')
        str << " WHERE #{where}"

        str
    end

    def update(table, values, where, federate)
        sql = update_sql(table, values, where)
        db_exec(sql, "Error updating record", federate)
    end

    def update_body_sql(table, body, where)
        "UPDATE #{table} SET body = '#{db_escape(body)}' WHERE #{where}"
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

    def percentage_line(current, max, carriage_return = false)
        return_symbol = carriage_return ? "\r" : ""
        percentile = current.to_f / max.to_f * 100

        "#{current}/#{max} #{percentile.round(2)}%#{return_symbol}"
    end

    def purge_history(options = {})
        vmpool = OpenNebula::VirtualMachinePool.new(client)
        vmpool.info_all

        ops = {
            start_time: 0,
            end_time: Time.now
        }.merge(options)

        start_time  = ops[:start_time].to_i
        end_time    = ops[:end_time].to_i

        last_id = vmpool["/VM_POOL/VM[last()]/ID"]

        vmpool.each do |vm|
            print percentage_line(vm.id, last_id, true)

            time = vm["STIME"].to_i
            next unless time >= start_time && time < end_time

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

            if Array === val_history && val_history.size > 2
                last_history = val_history.last(2)

                old_seq = []
                seq_num = last_history.first['SEQ']

                # Renumerate the sequence
                last_history.each_with_index do |history, index|
                    old_seq << history['SEQ'].to_i
                    history['SEQ'] = index
                end

                vm.delete_element('HISTORY_RECORDS/HISTORY')
                vm.add_element('HISTORY_RECORDS', 'HISTORY' => last_history)

                # Update VM body to leave only the last history record
                body = db_escape(vm.to_xml)
                update_body("vm_pool", vm.to_xml, "oid = #{vm.id}", false)

                # Delete any history record that does not have the same
                # SEQ number as the last history record
                pp seq_num
                delete("history", "vid = #{vm.id} and seq < #{seq_num}", false)

                # Renumerate sequence numbers
                old_seq.each_with_index do |seq, index|
                    update("history",
                           { seq: index },
                           "vid = #{vm.id} and seq = #{seq}", false)
                end
            end
        end
    end

    def purge_done_vm(options = {})
        vmpool = OpenNebula::VirtualMachinePool.new(client)
        vmpool.info(OpenNebula::Pool::INFO_ALL,
                    -1,
                    -1,
                    OpenNebula::VirtualMachine::VM_STATE.index('DONE'))

        ops = {
            start_time: 0,
            end_time: Time.now
        }.merge(options)

        start_time  = ops[:start_time].to_i
        end_time    = ops[:end_time].to_i

        last_id = vmpool["/VM_POOL/VM[last()]/ID"]

        vmpool.each do |vm|
            print percentage_line(vm.id, last_id, true)

            time = vm["ETIME"].to_i
            next unless time >= start_time && time < end_time

            delete("vm_pool", "oid = #{vm.id}", false)
            delete("history", "vid = #{vm.id}", false)
        end
    end
end
