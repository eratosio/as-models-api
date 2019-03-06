all_port_types_model <- function(context) {
    cat('Running all_port_types_model')

    input_documents = context$ports$input_documents
    output_documents = context$ports$output_documents

    #for (input in input_documents) {
    #   output_documents[[input$index + 1]]$document = paste(input$document, input$index, sep=' ')
    #}

    context$update(modified_documents=list(output_document=paste0(context$ports$input_document$document,' updated')))

    # for collection ports might be easier to implement something like:?
     #context$update(context)

    cat('Done all_port_types_model')

}

test_model <- function(context) {
    cat('Running test_model')
    cat('Done test_model')

}

required_ports_model_in1_out1 <- function(context) {
    cat('Running required_ports_model_in1_out1')
    cat('Done required_ports_model_in1_out1')

}