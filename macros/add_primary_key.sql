{% macro add_primary_key(variable_name) -%}

    ALTER TABLE {{ this.name }} ADD PRIMARY KEY ( {{ variable_name }} );

{%- endmacro %}