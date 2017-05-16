require 'faker'
require 'date'
require 'csv'
require 'geocoder'

@now = Date.today

def create_full_address
    full_street_address = Faker::Address.street_address + " " + Faker::Address.street_suffix
    return full_street_address
end

def create_random_date(days)
  random_value = rand(1...days)
  random_date = (@now - random_value)
  return random_date
end

def create_people(num)
  people = []
  num.times do
    people.push [
      Faker::Name.first_name,
      Faker::Name.last_name,
      Faker::Demographic.race,
      Faker::Date.birthday
    ]
  end
  return people
end
