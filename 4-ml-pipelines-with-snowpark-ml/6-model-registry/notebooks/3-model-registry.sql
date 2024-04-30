-- see in Snowsight first!

show models
    in schema test.public;

show versions
    in model test.public.RANDOMFORESTREGRESSOR;

alter model test.public.RANDOMFORESTREGRESSOR
    set comment = 'new comment';

show models
    like '%RANDOMFORESTREGRESSOR%'
    in schema test.public;

-- drop model test.public.RANDOMFORESTREGRESSOR;
