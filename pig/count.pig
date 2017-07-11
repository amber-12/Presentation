book = load '$input' using textloader() as (lines:chararray);

--dump book;

transform = foreach book generate FLATTEN (TOKENIZER(lines)) as word;

--dump transform;

transform = foreach transform generate TRIM(LOWER(REPLACE(word,'[\\p{punct},\\p{cntrl}]',''))) as word;

transform = filter transform by word =='$myword' ;

--dump transform;

groupbyword = group transform by word;


countofeachword = foreach groupbyword generate group ,COUNT(transform);

dump countofeachword;

