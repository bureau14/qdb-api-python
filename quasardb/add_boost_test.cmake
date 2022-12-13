function(add_boost_test_executable NAME COMPONENT)
    add_executable(${NAME}
        ${ARGN}
    )

    # set_target_properties(${NAME} PROPERTIES
    #     DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

    target_link_libraries(${NAME}
        boost_unit_test_framework
    )

    if($<CONFIG:SanitizeValgrind>)
        add_test(
            NAME ${NAME}
            COMMAND
                valgrind>
                $<$<PLATFORM_ID:Darwin>:--dsymutil=yes>
                --gen-suppressions=all
                ./${NAME}
                --log_level=test_suite
                --report_level=detailed
                --build_info=yes
                --detect_memory_leaks=0
        )
    else()
        add_test(
            NAME ${NAME}
            COMMAND
                ${NAME}
                --log_level=test_suite
                --report_level=detailed
                --build_info=yes
                --detect_memory_leaks=0
        )
    endif()

    install(TARGETS ${NAME}
        RUNTIME DESTINATION bin COMPONENT ${COMPONENT}
        LIBRARY DESTINATION lib COMPONENT ${COMPONENT}
        ARCHIVE DESTINATION lib COMPONENT ${COMPONENT}
    )
endfunction()
