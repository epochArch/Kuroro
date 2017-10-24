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

package com.epocharch.kuroro.common.mongo;

import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.dao.CompensationDao;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class CompensationDAOImplTest extends AbstractDAOImplTest {

	@Autowired
	private CompensationDao compensationDao;

	@Autowired
	private MessageDAO messageDAO;

	@Autowired
	private AckDAO ackDAO;

	@Test
	public void testCompensationDao() {

		System.out
				.println(compensationDao.getCompensationId("archtest", "aaa", null));
	}

}
