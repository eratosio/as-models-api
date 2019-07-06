all_port_types_model <- function(context) {
    print('Running all_port_types_model')

    input_documents = context$ports$input_documents
    output_documents = context$ports$output_documents

    for (i in input_documents) {
        print(i)
    }

    for (o in output_documents) {
        print(o)
    }

    # TODO: currently writing back to document collection ports in R is not complete.
    # TODO: for collection ports it might be easier to implement something like:?
    # context$update(context)
    # for (input in input_documents) {
    #     output_documents[[input$index + 1]]$document = paste(input$document, input$index, sep=' ')
    # }

    context$update(modified_documents=list(output_document=paste0(context$ports$input_document$document,' updated')))

    print('Done all_port_types_model')

}

test_model <- function(context) {
    print('Running test_model')
    print('Done test_model')

}

required_ports_model_in1_out1 <- function(context) {
    print('Running required_ports_model_in1_out1')
    print('Done required_ports_model_in1_out1')

}

test_error <- function(context) {
    stop("something went wrong")
}