create table course as
with all_course as (
	select distinct
	    "subj" as subject,
	    "#" as number,
	    "title" as title
	from
	    semester_data
)
select
    gen_random_uuid () as id,
    subject,
    number,
    title
from
    all_course;

create table section as
with
    all_section as (
        select distinct
            "subj" as subject,
            "#" as number,
            "title" as title,
            "Comp Numb" as crn,
            "Sec" as sec,
            "Max Enrollment" as max_enrollment,
            "Current Enrollment" as current_enrollment
        from
            semester_data d
    )
select
    gen_random_uuid () as id,
    c.id as course_id,
    crn,
    sec,
    max_enrollment,
    current_enrollment
from
    all_section d
    left join course c on d.subject = c.subject
    and d.number = c.number
    and d.title = c.title;

create table section_block as
with
    semester_by_day as (
        select distinct
            "comp numb" as crn,
            "sec" as sec,
            "bldg" as building,
            "room" as room,
            "instructor" as instructor,
            coalesce(try_cast ("Start Time" as time), '00:00:00') as start_time,
            coalesce(try_cast ("End Time" as time), '00:00:00') as end_time,
            unnest (
                string_split (
                    coalesce(
                        nullif(replace(days, ' ', ''), ''),
                        'X'
                    ),
                    ''
                )
            ) as day
        from
            semester_data
    )
select
    s.id as section_id,
    sbd.*
from
    semester_by_day sbd
    join section s on sbd.crn = s.crn
    and sbd.sec = s.sec;

create table catalog_section as (
    with blocks_by_day as (
        select
            s.id as section_id,
            b.day,
            json_group_array(
                json_object(
                    'crn', b.crn,
                    'sec', b.sec,
                    'building', b.building,
                    'room', b.room,
                    'instructor', b.instructor,
                    'start_time', b.start_time,
                    'end_time', b.end_time
                )
            ) as day_blocks
        from
            section s
            left join section_block b on s.id = b.section_id
        group by
            s.id, b.day
    )
    select
        s.id as section_id,
        s.crn,
        s.sec,
        s.course_id,
        coalesce(
            json_group_array(
                json_object(
                    'day', bbd.day,
                    'blocks', bbd.day_blocks
                )
            ),
            []
        ) as blocks
    from
        section s
        left join blocks_by_day bbd on s.id = bbd.section_id
    group by
        s.id, s.crn, s.sec, s.course_id
);

create table catalog as (
    select
        json_object(
            'id', c.id,
            'subject', c.subject,
            'number', c.number,
            'title', c.title,
            'description', coalesce(cd.description, ''),
            'sections', coalesce(
                json_group_array(
                    json_object(
                        'id', s.section_id,
                        'crn', s.crn,
                        'sec', s.sec,
                        'days', coalesce(s.blocks, [])
                    )
                ),
                []
            )
        ) as 'course'
    from
        course c left join course_desc cd on c.subject = cd.subject and c.number = cd.number
                 left join catalog_section s on c.id = s.course_id
    group by
        c.id, c.subject, c.number, c.title, cd.description
    order by
        c.id, c.subject, c.number
);
