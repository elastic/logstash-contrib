require 'logstash/namespace'
require 'logstash/filters/base'

# A filter that applies a set of arithmetic operation on two fields.
#
# The result is stored in a field provided by the user.
#
# Requires the field value to be a number (as string).
#
# Example config:
#   arithop {
#     left_operand_field => "count"
#     right_operand_field => "mean"
#     operation => "*"
#     target_field => "total_time"
#   }
#
class LogStash::Filters::ArithOp < LogStash::Filters::Base
  config_name 'arithop'
  milestone 2

  # TODO: Instaed of left/right op we could give a vector
  # of fields that would then be reduced with an arithmetic operation.

  # The left operand.
  config :left_operand_field, :validate => :string, :required => true

  # The right opearnd.
  config :right_operand_field, :validate => :string, :required => true

  # The operation to be applied. One of: +, -, *, /
  config :operation, :validate => :string, :required => true

  # The field where the result shall be stored.
  config :target_field, :validate => :string, :required => true

  @@books = {
    "+" => lambda { |lop, rop| lop + rop },
    "-" => lambda { |lop, rop| lop - rop },
    "*" => lambda { |lop, rop| lop * rop },
    "/" => lambda { |lop, rop|
      if rop == 0
        0
      else
        lop / rop
      end
      },
  }

  public
  def register
  end

  public
  def filter(event)
    lop = str_as_float(event[@left_operand_field])
    rop = str_as_float(event[@right_operand_field])
    event[@target_field] = @@books[@operation].call(lop, rop)
  end

  def str_as_float(str)
    Float(str) rescue nil
  end

end # class LogStash::Filters::ArithOp
