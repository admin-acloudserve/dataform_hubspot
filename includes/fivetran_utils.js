function enabled_vars (vars) {
    for (let i = 0; i < vars.length; i++) {
        if(vars[i] != true) {
            return false
        }
    }
    return true
}

function add_pass_through_columns (base_columns, pass_through_var) {
    if (pass_through_var){
        for (let i = 0; i < pass_through_var.length; i++) {
            base_columns.push(pass_through_var[i])
        }
    }
    return base_columns
}


function fill_staging_columns (source_columns) {

    let column = []
    for (let i = 0; i < source_columns.length; i++) {
        let values = Object.values(source_columns[i]);
        let keys = Object.keys(source_columns[i]);
        // if the key contains alias 
        if (keys.includes("alias")) {
            column.push(
                values[0].toLowerCase().concat(' as ').concat(source_columns[i]['alias'].toLowerCase())
                
            )
        }
        else {
            column.push(values[0].toLowerCase())
        }
    }
    console.log(column)
    return column
}


 module.exports = { 
     enabled_vars,
     add_pass_through_columns,
     fill_staging_columns
};