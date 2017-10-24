/*
 * Copyright 2017 EpochArch.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epocharch.kuroro.broker;

import com.epocharch.kuroro.broker.leaderserver.Compensator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * @author bill
 * @date 5/6/14
 */
@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext-test.xml")
public class CompensationTest extends AbstractJUnit4SpringContextTests {

	static {
		System.setProperty("global.config.path",
				"/Users/bill/Documents/work/env/test");
	}

	@Autowired
	private Compensator compensator;

	@Before
	public void setUp() {

	}

	@Test
	public void test() {
		compensator.start();
//		compensator.compensate("archtest", "compensationTemp");

	}

	@After
	public void sleep() throws InterruptedException {
		Thread.sleep(20 * 1000);
		// compensator.stopCompensation("archtest","archtestConsumer");
		Thread.sleep(1000 * 1000);
	}
}
