require 'spec_helper'

DB.extension :redshift_schema_dumper

describe Sequel::Redshift::SchemaDumper do
  let(:options) { {distkey: :region, sortkeys: [:colour, :crunchiness]} }
  let(:create_macro) do
    [create_table,
     '  String :region, :size=>255',
     '  String :crunchiness, :size=>255',
     '  String :colour, :size=>255',
     'end',].join("\n")
  end

  before do
    DB.create_table!(:apples, options) do
      String :region
      String :crunchiness
      String :colour
    end
  end

  describe '#dump_table_schema' do
    subject { DB.dump_table_schema(:apples) }

    context 'with distkey and sortkeys' do
      let(:create_table) { 'create_table!(:apples, :distkey=>:region, :sortkeys=>[:colour, :crunchiness]) do' }
      it 'should output the distkey and sortkeys' do
        is_expected.to eq create_macro
      end
    end

    context 'no diskey or sortkeys' do
      let(:options) { {} }
      let(:create_table) { 'create_table!(:apples) do' }
      it { is_expected.to eq create_macro }
    end

    context 'with sortstyle' do
      let(:options) { {distkey: :region, sortkeys: [:colour, :region, :crunchiness], sortstyle: :interleaved} }
      let(:create_table) { 'create_table!(:apples, :distkey=>:region, :sortkeys=>[:colour, :region, :crunchiness], :sortstyle=>:interleaved) do' }
      it { is_expected.to eq create_macro }
    end
  end

  describe '#dump_schema_migration' do
    it 'should not blow up' do
      expect(DB.dump_schema_migration).to be_a String
    end
  end
end
